use config;
use super::length_delimited::LengthDelimited;
use super::junkify::Junkify;

use tokio_core::io::{Io, FramedIo};
use tokio_proto::multiplex::RequestId;
use tokio_timer::Timer;

use futures::{Async, Poll};

use buffoon::{self, Serialize, Deserialize, OutputStream, InputStream};
use std::io;
use std::marker::PhantomData;

/// Takes raw byte frames (from `LengthDelimted`) and performs protobuf
/// encoding / decoding.
///
/// A `Transport` instance requires a single message type for each direction
/// (in & out). In the case of MiniDB, this message type will be
/// `proto::Request` and `proto::Response`. The specific message type may be an
/// enumeration of sub types.
///
/// `Transport` also associates a request ID with each message. This enables
/// compatibility w/ tokio_proto::easy::multiplex. Since responses may not be
/// ordered the same as their matching requests, the request ID enables linking
/// the two.
pub struct Transport<T: Io, U, V> {
    // The upstream I/O will have an initial framing pass done by
    // `LengthDelimited`. The upstream is also decorated with `Junkify` which
    // simulates poor network connectivity.
    //
    // It is not recommended to run `Junkify` in a real production service...
    upstream: Junkify<LengthDelimited<T>>,

    // The message type written to the upstream
    in_msg: PhantomData<U>,

    // The message type read from the upstream
    out_msg: PhantomData<V>,
}

// Tagged pairs a protobuf message with a request ID. We leverage protobufs for
// serializing / deserializing the request ID with the message.
struct Tagged<T> {
    request_id: RequestId,
    message: T,
}

impl<T: Io, U, V> Transport<T, U, V> {

    /// Create a new `Transport` backed by the given `upstream`
    pub fn new(upstream: T) -> Transport<T, U, V> {
        Transport::new2(upstream, None, None)
    }

    /// Create a new `Transport` backed by the given `upstream`. The returned
    /// `Transport` will be "junkified" in order to simulate poor network
    /// connectivity.
    pub fn junkified(upstream: T, route: &config::Route, timer: &Timer) -> Transport<T, U, V> {
        Transport::new2(upstream, Some(route), Some(timer))
    }

    fn new2(upstream: T, route: Option<&config::Route>, timer: Option<&Timer>) -> Transport<T, U, V> {
        // Construct the transport. This is done by composing the provided
        // `upstream` with `LenghtDelimited` and `Junkify`.
        let upstream = LengthDelimited::new(upstream);
        let upstream = Junkify::new(upstream, route, timer);

        Transport {
            upstream: upstream,
            in_msg: PhantomData,
            out_msg: PhantomData,
        }
    }
}

impl<T, U, V> FramedIo for Transport<T, U, V>
    where T: Io,
          U: Serialize,
          V: Deserialize,
{
    // The type read from the socket: A tuple of the request ID and the message
    // type.
    type In = (RequestId, U);

    // The type written to the socket. A tuple of the request ID and the
    // message type. `None` represents a clean shutdown of the upstream (socket
    // closed).
    type Out = Option<(RequestId, V)>;

    // `Transport` is readable when the upstream is readable
    fn poll_read(&mut self) -> Async<()> {
        self.upstream.poll_read()
    }

    fn read(&mut self) -> Poll<Self::Out, io::Error> {
        trace!("Transport::read");

        // Attempt to read a message from the upstream.
        //
        // First, try to read an upstream frame. The returned value is a
        // `Vec<u8>` containing the encoded protobuf message.
        //
        // `try_ready!` will return from the function when the upstream returns
        // `NotReady` or `Err`.
        let data = match try_ready!(self.upstream.read()) {
            Some(data) => data,
            None => {
                // In the case that the upstream is shutdown, just return
                // `None`.
                trace!("   --> done");
                return Ok(Async::Ready(None));
            }
        };

        trace!("   --> got data");

        // Try deserializing the message
        let tagged: Tagged<V> = try!(buffoon::deserialize(&mut io::Cursor::new(data)));

        trace!("   --> deserialized; message-id={}", tagged.request_id);

        // Return the result
        Ok(Async::Ready(Some((tagged.request_id, tagged.message))))
    }

    // `Transport` is writable when the upstream is writable
    fn poll_write(&mut self) -> Async<()> {
        self.upstream.poll_write()
    }

    fn write(&mut self, (id, message): Self::In) -> Poll<(), io::Error> {
        trace!("Transport::write; message-id={}", id);

        // Create the tagged message
        let tagged = Tagged {
            request_id: id,
            message: message,
        };

        // Serialize the tagged message using buffoon, this will return a
        // `Vec<u8>`
        let data = try!(buffoon::serialize(&tagged));

        // Write the encoded message to the upstream.
        self.upstream.write(data)
    }

    fn flush(&mut self) -> Poll<(), io::Error> {
        trace!("Transport::flush");

        // Flush the upstream to the socket. This will actually perform the
        // write.

        self.upstream.flush()
    }
}

impl<T: Serialize> Serialize for Tagged<T> {
    fn serialize<O: OutputStream>(&self, out: &mut O) -> io::Result<()> {
        try!(out.write(1, &self.request_id));
        try!(out.write(2, &self.message));
        Ok(())
    }
}

impl<T: Deserialize> Deserialize for Tagged<T> {
    fn deserialize<R: io::Read>(i: &mut InputStream<R>) -> io::Result<Tagged<T>> {
        let mut request_id = None;
        let mut message = None;

        while let Some(f) = try!(i.read_field()) {
            match f.tag() {
                1 => request_id = Some(try!(f.read())),
                2 => message = Some(try!(f.read())),
                _ => try!(f.skip()),
            }
        }

        Ok(Tagged {
            request_id: required!(request_id),
            message: required!(message),
        })
    }
}
