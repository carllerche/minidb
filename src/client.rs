//! Client interface to MiniDB cluster
//!
//! API for communicating with a MiniDB cluster. Enables issuing requests to
//! the cluster and getting the responses.
//!
//! The client API is blocking. It uses `Future::wait` to take an asynchronous
//! future and block the current thread until the response is complete.

use dt::{Set, VersionVec};
use proto::{self, Request, Response, Transport};

use tokio_core::reactor::Core;
use tokio_core::net::TcpStream;
use tokio_service::Service;
use tokio_proto::easy::{multiplex, EasyClient};

use futures::{self, Future, Complete};

use std::io;
use std::net::SocketAddr;

/// MiniDB client handle
pub struct Client {
    service: EasyClient<Request, Response>,
    // When this `Complete` is dropped, the reactor will shutdown.
    _shutdown_tx: Complete<()>,
}

impl Client {

    /// Establish a connection with a MiniDB server
    ///
    /// The function spawns a reactor thread.
    pub fn connect(addr: &SocketAddr) -> io::Result<Client> {
        use std::thread;
        use std::sync::mpsc;

        // The reactor core is not `Send`, so in order to spawn a new reactor
        // core on another thread and get back a remote handle to it, it takes
        // a little bit of effort;
        //
        // A thread will be spawned that will initialize the client. The client
        // handle must then be sent back to this thread. This channel
        // transports the client from the reactor thread back to this thread
        let (client_tx, client_rx) = mpsc::channel();

        // Use this oneshot as a gate to shutdown the reactor. When
        // `shutdown_tx` is dropped, the future will complete.
        let (shutdown_tx, shutdown_rx) = futures::oneshot::<()>();

        // Clone the SocketAddr so that it can be moved into the reactor thread.
        let addr = addr.clone();

        // Spawn the reactor thread
        thread::spawn(move || {

            // Create the tokio-core reactor
            let mut core = match Core::new() {
                Ok(core) => core,
                Err(e) => {
                    // Failed to create the reactor, send back an error
                    client_tx.send(Err(e)).unwrap();
                    return;
                }
            };

            // Get a handle to the reactor
            let handle = core.handle();

            // Connect the Tcp socket to the MiniDB server node
            let sock = TcpStream::connect(&addr, &handle);

            // Wait until the socket is connected
            let task = sock.then(move |res| {
                // Send the client handle back to the original thread
                client_tx.send(res.map(|sock| {
                    // The socket has been established, initialize the
                    // transport and create the client handle
                    let transport = Transport::new(sock);
                    multiplex::connect(transport, &handle)
                })).unwrap();

                Ok(())
            });

            // Spawn connect task to drive the connection
            core.handle().spawn(task);

            // Run the reactor core until the shutdown gate toggles
            core.run(shutdown_rx).unwrap();
        });

        // Block until the client has been established and sent back
        let client = try!(client_rx.recv().unwrap());

        Ok(Client {
            service: client,
            _shutdown_tx: shutdown_tx,
        })
    }

    /// Get the set of values from the server
    pub fn get(&self) -> io::Result<Set<String>> {
        let request = Request::Get(proto::Get);

        self.service.call(request).wait()
            .map(|resp| {
                match resp {
                    Response::Value(set) => set,
                    _ => unreachable!(),
                }
            })
    }

    /// Insert a value into the distributed set
    pub fn insert<T: Into<String>>(&self, value: T) -> io::Result<()> {
        let request = Request::Insert(proto::Insert::from(value));

        try!(self.service.call(request).wait());
        Ok(())
    }

    /// Remove a value from the distributed set
    pub fn remove<T: Into<String>>(&self, value: T) -> io::Result<()> {
        let request = Request::Remove(proto::Remove::from(value));

        try!(self.service.call(request).wait());
        Ok(())
    }

    /// Issue a causal remove
    ///
    /// Causal removes will remove a value from the set represented by the
    /// given version vec
    pub fn causal_remove<T: Into<String>>(&self, value: T, version_vec: &VersionVec) -> io::Result<()> {
        let remove = proto::Remove::new(value.into(), Some(version_vec.clone()));
        let request = Request::Remove(remove);

        try!(self.service.call(request).wait());
        Ok(())
    }

    /// Clear all values from the set
    pub fn clear(&self) -> io::Result<()> {
        let request = Request::Clear(proto::Clear);

        try!(self.service.call(request).wait());
        Ok(())
    }
}
