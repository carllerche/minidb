use config;
use dt::{Set};
use proto::{Request, Response, Transport};

use tokio_core::channel::{channel, Sender, Receiver};
use tokio_core::reactor::Handle;
use tokio_core::net::TcpStream;
use tokio_service::Service;
use tokio_proto::easy::{EasyClient, multiplex};
use tokio_timer::{Timer, Sleep};

use futures::{Future, Poll, Async};
use futures::stream::Stream;

use std::{io, mem};
use std::time::Duration;

// Handle to the peer task.
//
// Sending a join message to a peer dispatches a message on `tx` to the task
// managing the peer connection and will be processed there.
//
// See `Task` for details on the peer task.
pub struct Peer {
    tx: Sender<Set<String>>,
}

// State required for managing a peer connection.
//
// Connections to MiniDB peers are managed on reactor tasks. When the server
// initializes, it spawns one task for each peer in the cluster. The peer task
// is responsible for maintaining an open connection to the peer and to send a
// `Join` message every time the state is sent to the task.
//
// If the connection fails, the task will attempt a reconnect after a short
// period of time.
struct Task {
    // Receives `Set` values that need to be sent to the peer.
    rx: Receiver<Set<String>>,

    // Route information
    route: config::Route,

    // Tokio reactor handle. Used to establish tcp connections
    reactor_handle: Handle,

    // Handle to the timer. The timer is used to set a re-connect timeout when
    // the peer tcp connection fails.
    timer: Timer,

    // Current tcp connection state, see below
    state: State,

    // Pending `Join` message to send. This also tracks in-flight joins. If a
    // join request to a peer fails, the connection will be re-established.
    // Once it is re-established, the join request should be sent again.
    //
    // However, if while the task is waiting to re-establish a connection, a
    // new state is replicated, then drop the join request that failed to send
    // in favor of the newer one. Doing so is safe thanks to CRDTs!
    pending_message: PendingMessage,

    // Pending response future. A join was issued to the peer and the task is
    // currently waiting for the response.
    pending_response: Option<Box<Future<Item = Response, Error = io::Error>>>,
}

// Peer connection state. The actual connection to the peer node can be in one
// of the following states:
enum State {

    // Waiting to connect, this state is reached after hitting a connect error
    Waiting(Sleep),

    // Connecting to the remote. A TCP connect has been issued and the task is
    // waiting on the connect to complete
    Connecting(Box<Future<Item = TcpStream, Error = io::Error>>),

    // A connection is open to the peer.
    Connected(EasyClient<Request, Response>),
}

// Tracks the state of replication requests
enum PendingMessage {
    // A replication request is waiting to be sent.
    Pending(Set<String>),

    // A replication request is currently in-flight. The value of the message
    // is saved in case the request fails and must be re-issued later.
    InFlight(Set<String>),

    // There are no pending replication requests.
    None,
}

impl Peer {

    /// Establish a connection to a peer node.
    pub fn connect(route: config::Route, handle: &Handle, timer: &Timer) -> Peer {
        // Create a channel. The channel will be used to send replication
        // requests from the server task to the peer task.
        let (tx, rx) = channel(handle).unwrap();

        // Initialize the task state
        let task = Task {
            rx: rx,
            route: route,
            reactor_handle: handle.clone(),
            timer: timer.clone(),

            // Initialize in the "waiting to connect" state but with a 0 length
            // sleep. This will effectively initiate the connect immediately
            state: State::Waiting(timer.sleep(Duration::from_millis(0))),

            // There are no pending messages
            pending_message: PendingMessage::None,

            // There are no pending responses
            pending_response: None,
        };

        // Spawn the task
        handle.spawn(task);

        // Return the send half as the peer handle
        Peer { tx: tx }
    }

    // Send a replication request to the task managing the peer connection
    pub fn send(&self, set: Set<String>) {
        self.tx.send(set).unwrap();
    }
}

// Implement `Future` for `Task`. All tasks spawned on the I/O reactor must
// implement future w/ Item = () and Error = ();
impl Future for Task {
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<(), ()> {
        // First, process any in-bound replication requests
        self.process_rx();

        // Perform pending work.
        try!(self.tick());

        Ok(Async::NotReady)
    }
}

impl Task {
    fn process_rx(&mut self) {
        // Read any pending replication request and set `pending_message`. It
        // is expected that some messages will be dropped. The most important
        // thing is that the **last** replication request ends up getting
        // processed.
        while let Async::Ready(Some(set)) = self.rx.poll().unwrap() {
            self.pending_message = PendingMessage::Pending(set);
        }
    }

    fn tick(&mut self) -> Poll<(), ()> {
        trace!("Peer::tick; actor-id={:?}", self.route.destination());

        loop {
            match self.state {
                State::Waiting(..) => {
                    // Currently waiting a short period of time before
                    // establishing the TCP connection with the peer.
                    try_ready!(self.process_waiting());
                }
                State::Connecting(..) => {
                    // Waiting for the TCP connection finish connecting
                    try_ready!(self.process_connecting());
                }
                State::Connected(..) => {
                    if self.pending_response.is_some() {

                        // A request has been sent, waiting for the response.
                        try_ready!(self.process_response());

                    } else if self.pending_message.is_some() {

                        // A join request is pending, dispatch it
                        try_ready!(self.process_connected());

                    } else {

                        // Nothing to do, return ok
                        return Ok(Async::Ready(()));

                    }
                }
            }
        }
    }

    fn process_waiting(&mut self) -> Poll<(), ()>  {
        trace!("   --> waiting");

        match self.state {
            // Try polling the sleep future. If `NotReady` `process_waiting`
            // will return.
            State::Waiting(ref mut sleep) => try_ready!(sleep.poll()),
            _ => unreachable!(),
        }

        // We are done waiting and can now attempt to establish the connection

        trace!("   --> sleep complete -- attempting tcp connect");

        // Start a tcp connect
        let socket = TcpStream::connect(&self.route.remote_addr(), &self.reactor_handle);

        // Set a connect timeout of 5 seconds
        let socket = self.timer.timeout(socket, Duration::from_secs(5));

        // Transition the state to "connecting"
        self.state = State::Connecting(Box::new(socket));

        Ok(Async::Ready(()))
    }

    fn process_connecting(&mut self) -> Poll<(), ()>  {
        trace!("   --> connecting");

        // Check if the `connecting` future is complete, aka the connection has
        // been established
        let socket = match self.state {
            State::Connecting(ref mut connecting) => {
                match connecting.poll() {
                    // The connection is not yet established
                    Ok(Async::NotReady) => return Ok(Async::NotReady),

                    // The connection is established
                    Ok(Async::Ready(socket)) => Some(socket),

                    // An error was hit while connecting. A timeout for a short
                    // period of time will be set after which, the connect will
                    // be stablished again.
                    Err(err) => {
                        info!("failed to connect to {}; attempting again in 5 seconds; err={:?}", self.route.remote_addr(), err);
                        None
                    }
                }
            }
            _ => unreachable!(),
        };

        if let Some(socket) = socket {
            trace!("   --> connect success");
            info!("established peer connection to {:?}", self.route.remote_addr());

            // The connection was successfully established. Now we have a Tcp
            // socket. Using that, we will build up the MiniDB transport.
            //
            // The socket will be wrapped by the length delimited framer,
            // followed by junkify, and last `Transport`.
            let transport = Transport::junkified(socket, &self.route, &self.timer);

            // Using the transport, spawn a task that manages this connection
            // (vs. the general peer replication task).
            //
            // This is done with `tokio-proto`, which takes the transport and
            // returns a `Service`. Requests can be dispatched directly to the
            // service.
            let service = multiplex::connect(transport, &self.reactor_handle);

            // Update the state
            self.state = State::Connected(service);
        } else {
            trace!("   --> connect failed");

            // The connection failed, transition the state to "waiting to
            // reconnect". We will wait a short bit of time before attempting a
            // reconnect.
            self.transition_to_waiting();
        }

        Ok(Async::Ready(()))
    }

    fn process_connected(&mut self) -> Poll<(), ()>  {
        trace!("   --> process peer connection");

        let service = match self.state {
            State::Connected(ref mut service) => service,
            _ => unreachable!(),
        };

        // The connection is currently in the connected state. If there are any
        // pending replication requests, then they should be dispatched to the
        // client.

        // First ensure that the service handle is ready to accept requests, if
        // not, return `NotReady` and try again later.
        if !service.poll_ready().is_ready() {
            trace!("   --> peer socket not ready");
            return Ok(Async::NotReady);
        }

        // Build the join / replication request
        let set = self.pending_message.message_to_send().unwrap();
        let msg = Request::Join(set);

        trace!("   --> sending Join message");

        // Dispatch the replication request and get back a future repesenting
        // the response from the peer node.
        let resp = service.call(msg);

        // Timeout the response after 5 seconds. If the peer does not
        // respond to the join within this time, the connection will be
        // reestablished and the join sent again
        let resp = self.timer.timeout(resp, Duration::from_secs(5));

        // Track the response future
        self.pending_response = Some(Box::new(resp));

        Ok(Async::Ready(()))
    }

    fn process_response(&mut self) -> Poll<(), ()> {
        trace!("   --> process peer response");

        // Check the response future. If it is complete, see if it is a
        // successful response or if the connection needs to be re-established

        let response = match self.pending_response {
            Some(ref mut pending_response) => {
                match pending_response.poll() {
                    Ok(Async::Ready(v)) => Ok(v),
                    Err(e) => Err(e),
                    Ok(Async::NotReady) => return Ok(Async::NotReady),
                }
            }
            _ => unreachable!(),
        };

        // Clear the pending response future
        self.pending_response = None;

        // The response has completed, check to see if it was successful
        match response {
            Ok(_) => {
                // The join / replication successfully applied
                self.pending_message.in_flight_succeeded();
                trace!("   --> received response: OK");
            }
            Err(e) => {
                // The replication failed. Transition the state to waiting to
                // connect. Also, setup the join request to get redisptached
                // once the connection is established again.
                warn!("message send failed to remote {:?} -- attempting reconnect in 5 seconds; err={:?}", self.route.remote_addr(), e);
                self.pending_message.in_flight_failed();
                self.transition_to_waiting();
            }
        }

        Ok(Async::Ready(()))
    }

    fn transition_to_waiting(&mut self) {
        trace!("waiting for 5 seconds before reconnecting; actor-id={:?}", self.route.destination());

        // Set a timeout for 5 seconds
        let sleep = self.timer.sleep(Duration::from_secs(5));

        // Update the state to reflect waiting
        self.state = State::Waiting(sleep);
    }
}

impl PendingMessage {
    fn is_none(&self) -> bool {
        match *self {
            PendingMessage::None => true,
            _ => false,
        }
    }

    fn is_some(&self) -> bool {
        !self.is_none()
    }

    fn message_to_send(&mut self) -> Option<Set<String>> {
        match mem::replace(self, PendingMessage::None) {
            PendingMessage::Pending(set) => {
                *self = PendingMessage::InFlight(set.clone());
                Some(set)
            }
            _ => None,
        }
    }

    fn in_flight_succeeded(&mut self) {
        match *self {
            PendingMessage::Pending(..) => return,
            _ => *self = PendingMessage::None,
        }
    }

    fn in_flight_failed(&mut self) {
        match mem::replace(self, PendingMessage::None) {
            PendingMessage::InFlight(set) => {
                *self = PendingMessage::Pending(set);
            }
            v => *self = v,
        }
    }
}
