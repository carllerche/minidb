//! Processes requests from clients & peer nodes
//!
//! # Overview
//!
//! The MinDB server is a peer in the MiniDB cluster. It is initialized with a
//! port to bind to and a list of peers to replicate to. The server then
//! processes requests send from both clients and peers.
//!
//! # Peers
//!
//! On process start, the cluster topology is read from a config file
//! (`etc/nodes.toml`). The node represented by the current server process is
//! extracted this topology, leaving the list of peers.
//!
//! After the server starts, connections will be established to all the peers.
//! If a peer cannot be reached, the connection will be retried until the peer
//! becomes reachable.
//!
//! # Replication
//!
//! When the server receives a mutation request from a client, the mutation is
//! processed on the server itself. Once the mutation succeeds, the server
//! state is replicated to all the peers. Replication involves sending the
//! entire data set to all the peers. This is not the most efficient strategy,
//! but we aren't trying to build Riak (or even MongoDB).
//!
//! In the event that replication is unable to keep up with the mutation rate,
//! replication messages are dropped in favor of ensuring that the final
//! replication message is delivered. This works because every replication
//! message includes the entire state at that point. The CRDT ensures that no
//! matter what state the peers are at, it is able to converge with the final
//! replication message (assuming no bugs in the CRDT implementation);

use config;
use dt::{Set, ActorId};
use peer::Peer;
use proto::{self, Request, Response, Transport};

use tokio_core::reactor::{Core, Handle};
use tokio_core::net::TcpListener;
use tokio_service::Service;
use tokio_proto::easy::multiplex;
use tokio_timer::Timer;

use futures::{self, Future, Async};
use futures::stream::{Stream};

use rand::{self, Rng};

use std::io;
use std::cell::RefCell;
use std::rc::Rc;

// The in-memory MinDB state. Stored in a ref-counted cell and shared across
// all open server connections. Whenever a socket connects, a `RequestHandler`
// instance will be initialized with a pointer to this.
struct Server {

    // The DB data. This is the CRDT Set. When the server process starts, this
    // is initialized to an empty set.
    data: Set<String>,

    // The server's ActorId. This ActorID value **MUST** be unique across the
    // cluster and should never be reused by another node.
    //
    // In theory, if the data was persisted to disk, the ActorId would be
    // stored withe persisted state. However, since the state is initialized
    // each time the process starts, the ActorId must be unique.
    //
    // To handle this, the ActorId is randomly generated.
    actor_id: ActorId,

    // Handle to the cluster peers. The `Peer` type manages the socket
    // connection, including initial connect as well as reconnecting when the
    // connection fails.
    //
    // Whenever the set is mutated by a client, it is replicated to the list of
    // peers.
    peers: Vec<Peer>,
}

// Handles MiniDB client requests. Implements `Service`
struct RequestHandler {
    server_state: Rc<RefCell<Server>>,
}

/// Run a server node.
///
/// The function initializes new server state, including an empty CRDT set,
/// then it will bind to the requested port and start processing connections.
/// Connections will be made from both clients and other peers.
///
/// The function will block while the server is running.
pub fn run(config: config::Node) -> io::Result<()> {
    // Create the tokio-core reactor
    let mut core = try!(Core::new());

    // Get a handle to the reactor
    let handle = core.handle();

    // Bind the Tcp listener, listening on the requested socket address.
    let listener = try!(TcpListener::bind(config.local_addr(), &handle));

    // `Core::run` runs the reactor. This call will block until the future
    // provided as the argument completes. In this case, the given future
    // processes the inbound TCP connections.
    core.run(futures::lazy(move || {
        // Initialize the server state
        let server_state = Rc::new(RefCell::new(Server::new(&config, &handle)));

        // `listener.incoming()` provides a `Stream` of inbound TCP connections
        listener.incoming().for_each(move |(sock, _)| {
            debug!("server accepted socket");

            // Create client handle. This implements `tokio_service::Service`.
            let client_handler = RequestHandler {
                server_state: server_state.clone(),
            };

            // Initialize the transport implementation backed by the Tcp
            // socket.
            let transport = Transport::new(sock);

            // Use `tokio_proto` to handle the details of multiplexing.
            // `EasyServer` takes the transport, which is basically a stream of
            // frames as they are read off the socket and manages mapping the
            // frames to request / response pairs.
            let connection_task = multiplex::EasyServer::new(client_handler, transport);

            // Spawn a new reactor task to process the connection. A task is a
            // light-weight unit of work. Tasks are generally used for managing
            // resources, in this case the resource is the socket.
            handle.spawn(connection_task);

            Ok(())
        })
    }))
}

impl Server {

    // Initialize the server state using the supplied config. This function
    // will also establish connections to the peers, which is why `&Handle` is
    // needed.
    fn new(config: &config::Node, handle: &Handle) -> Server {

        // Initialize a timer, this timer will be passed to all peers.
        let timer = Timer::default();

        // Connect the peers
        let peers = config.routes().into_iter()
            .map(|route| Peer::connect(route, handle, &timer))
            .collect();

        // Randomly assign an `ActorId` to the current process. It is
        // imperative that the `ActorId` is unique in the cluster and is not
        // reused across server restarts (since the state is reset).
        let mut rng = rand::thread_rng();
        let actor_id: ActorId = rng.next_u64().into();

        debug!("server actor-id={:?}", actor_id);

        // Return the new server state with an empty data set
        Server {
            data: Set::new(),
            actor_id: actor_id,
            peers: peers,
        }
    }

    // Replicate the current data set to the list of peers.
    fn replicate(&self) {
        // Iterate all the peers sending a clone of the data. This operation
        // performs a deep clone for each peer, which is not going to be super
        // efficient as the data set grows, but improving this is out of scope
        // for MiniDB.
        for peer in &self.peers {
            peer.send(self.data.clone());
        }
    }
}

// Service implementation for `RequestHandler`
//
// `Service` is the Tokio abstraction for asynchronous request / response
// handling. This is where we will process all requests sent by clients and
// peers.
//
// Instead of mixing a single service to handle both clients and peers, a
// better strategy would probably be to have two separate TCP listeners on
// different ports to handle the client and the peers.
impl Service for RequestHandler {

    // The Request and Response types live in `proto`
    type Request = Request;
    type Response = Response;
    type Error = io::Error;

    // For greates flexibility, a Box<Future> is used. This has the downside of
    // requiring an allocation and dynamic dispatch. Currently, the service
    // only responds with `futures::Done`, so the type could be changed.
    //
    // If the service respond with different futures depending on a conditional
    // branch, then returning Box or implementing a custom future is required.
    type Future = Box<Future<Item = Self::Response, Error = Self::Error>>;

    fn call(&self, request: Self::Request) -> Self::Future {
        match request {
            Request::Get(_) => {
                info!("[COMMAND] Get");

                // Clone the current state and respond with the set
                //
                let data = self.server_state.borrow().data.clone();
                let resp = Response::Value(data);

                Box::new(futures::done(Ok(resp)))
            }
            Request::Insert(cmd) => {
                info!("[COMMAND] Insert {:?}", cmd.value());

                // Insert the new value, initiate a replication to all peers,
                // and respond with Success
                //
                let mut state = self.server_state.borrow_mut();
                let actor_id = state.actor_id;
                state.data.insert(actor_id, cmd.value());

                // Replicate the new state to all peers
                state.replicate();

                let resp = Response::Success(proto::Success);

                Box::new(futures::done(Ok(resp)))
            }
            Request::Remove(cmd) => {
                info!("[COMMAND] Remove {:?}", cmd.value());

                let mut state = self.server_state.borrow_mut();
                let actor_id = state.actor_id;

                // If the request includes a version vector, this indicates
                // that a causal remove is requested. A causal remove implies
                // removing the value from the set at the state represented by
                // the version vector and leaving any insertions that are
                // either concurrent or successors to the supplied version
                // vector.
                match cmd.causality() {
                    Some(version_vec) => {
                        state.data.causal_remove(actor_id, version_vec, cmd.value());
                    }
                    None => {
                        state.data.remove(actor_id, cmd.value());
                    }
                }

                // Replicate the new state to all peers
                state.replicate();

                let resp = Response::Success(proto::Success);

                Box::new(futures::done(Ok(resp)))
            }
            Request::Clear(_) => {
                info!("[COMMAND] Clear");

                let mut state = self.server_state.borrow_mut();
                let actor_id = state.actor_id;
                state.data.clear(actor_id);

                state.replicate();

                let resp = Response::Success(proto::Success);

                Box::new(futures::done(Ok(resp)))
            }
            Request::Join(other) => {
                info!("[COMMAND] Join");

                // A Join request is issued by a peer during replication and
                // provides the peer's latest state.
                //
                // The join request is handled by joining the provided state
                // into the node's current state.

                let mut state = self.server_state.borrow_mut();
                state.data.join(&other);

                if log_enabled!(::log::LogLevel::Debug) {
                    for elem in state.data.iter() {
                        debug!("   - {:?}", elem);
                    }
                }

                let resp = Response::Success(proto::Success);

                Box::new(futures::done(Ok(resp)))
            }
        }
    }

    fn poll_ready(&self) -> Async<()> {
        Async::Ready(())
    }
}
