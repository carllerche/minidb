use tokio_core::io::{Io, FramedIo};
use tokio_proto::{TryRead, TryWrite};

use bytes::{MutBuf};
use bytes::buf::Take;
use byteorder::{NetworkEndian, ByteOrder};

use futures::{Async, Poll};
use std::{mem};
use std::io::{self, Cursor};

/// Frames data into chunks based on an integer length prefix.
///
/// Takes an upstream `Io` type and frames the data into chunks of bytes
/// (`Vec<u8>`) that are sized according to a 4-byte big-endian prefix.
///
/// In reverse, chunks of data written to this `FramedIo` are prefixed with a
/// length prefix when written to the upstream `Io` value.
pub struct LengthDelimited<T> {
    // Upstream I/O value
    upstream: T,
    // Current read state
    rd: Read,
    // Current write state
    wr: Write,
}

// `Take` is a buffer decorator provided by the `bytes` crate. It limits the
// inner buffer to a certain capacity. The inner buffer is `Vec<u8>` (a vector
// of bytes). Given that the vector is growable, the Vec<u8> capacity is
// unbounded.
//
// While processing the frames, we know how much data to expect. We know that
// the head is 4 bytes long and we know the length of the payload after reading
// the head. Thus, once the buffer has been filled to capacity, either the head
// or payload has been fully read.
type Buffer = Take<Vec<u8>>;

// Tracks the current read state
enum Read {
    // Currently reading the frame head, aka the 4 byte integer representing
    // the payload length.
    Head(Buffer),
    // Currently reading the payload
    Data(Buffer),
}

enum Write {
    // Currently not doing anything, ready to write a new frame
    Ready,
    // Writing the frame head, aka the 4 byte payload length integer. The
    // payload to write after the head is included.
    Head { head: Cursor<[u8; 4]>, data: Vec<u8> },
    // Writing the payload. The vector contains the full payload and the
    // `Cursor` points to the current write position in the vector.
    Data(Cursor<Vec<u8>>),
}

// A frame head consists of a network-endian 4 byte integer
const HEAD_LEN: usize = 4;

impl<T: Io> LengthDelimited<T> {

    /// Initialize the LengthDelimited framed I/O
    pub fn new(upstream: T) -> LengthDelimited<T> {
        LengthDelimited {
            upstream: upstream,
            rd: Read::head(),
            wr: Write::Ready,
        }
    }

    // Read the 4-byte frame head, returning the read value.
    //
    // * If the upstream returns `NotReady` before 4-bytes are read, the
    // `NotReady` is returned here.
    //
    // * If the upstream returns an error on read, the error is passed through.
    //
    // * If the upstream shutsdown, the function returns None **if** no data
    // has been buffered up yet. If data has been buffered (partial frame head
    // read), then an io error is returned.
    //
    // Finally, when the 4 bytes have been read successfully, they are parsed
    // and the frame payload length is returned.
    //
    // This function assumes that the current read state is `Read::Head`.
    //
    fn read_head(&mut self) -> Poll<Option<usize>, io::Error> {
        // Always operate in a loop, this ensures that the upstream is
        // processed until it returns NotReady.
        loop {
            // Get a reference to the buffer. This function should only be
            // called when the state is `Read::Head`, so panic otherwise.
            let buf = match self.rd {
                Read::Head(ref mut buf) => buf,
                _ => unreachable!(),
            };

            trace!("   --> remaining={}", buf.remaining());

            // The buffer has been limited to 4 bytes in capacity, once the
            // buffer has been filled, the head has been fully read. So, now
            // read the data as an integer and return the value.
            if !buf.has_remaining() {
                let bytes = buf.get_ref();

                assert_eq!(HEAD_LEN, bytes.len());
                let n = NetworkEndian::read_u32(bytes);

                return Ok(Async::Ready(Some(n as usize)));
            }

            // Try to fill the buffer from the upstream. `try_ready` will
            // return the current function if it gets an error or `NotReady`,
            // so if the upstream is not ready, `read_head` returns with
            // `NotReady` as well.
            let n = try_ready!(self.upstream.try_read_buf(buf));

            // If 0 bytes have been read, then the upstream has been shutdown.
            //
            // If the head buffer is empty, then this is a clean shutdown, in
            // which case return `None`. If the head was partially read, then
            // the upstream shutdown abruptly. Bubble this up with an error.
            if n == 0 {
                if buf.remaining() == HEAD_LEN {
                    return Ok(Async::Ready(None));
                } else {
                    return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "eof"));
                }
            }

            trace!("   --> read-head={}", n);
        }
    }

    // Similar to `read_head`, except that the length is determined by the
    // `read_head` return value.
    fn read_data(&mut self) ->  Poll<Option<Vec<u8>>, io::Error> {
        // Again operate in a loop ensuring that the upstream is processed
        // until it returns NotReady.
        loop {
            // Get the buffer
            let buf = match self.rd {
                Read::Data(ref mut buf) => buf,
                _ => unreachable!(),
            };

            // This time, if the buffer is full, break from the loop. This is
            // done to make the borrow checker happy as `buf` references the
            // state and must go out of scope before the buffer can be removed
            // from `self.rd`
            if !buf.has_remaining() {
                break;
            }

            // Try to fill the buffer, returning if an error or NotReady is
            // hit.
            let n = try_ready!(self.upstream.try_read_buf(buf));

            // Same as `read_head` except that the upstream should never
            // shutdown at this point, thus making a shutdown always an error.
            if n == 0 {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "eof"));
            }

            trace!("   --> read-data={}", n);
        }

        // Extract the buffer from the state, replacing the state with a new
        // 4-byte buffer to read the next head. Return the payload data
        match mem::replace(&mut self.rd, Read::head()) {
            Read::Data(buf) => Ok(Async::Ready(Some(buf.into_inner()))),
            _ => unreachable!(),
        }
    }

    // Write a frame head. This function will be called as part of
    // `FramedIo::flush`.
    fn write_head(&mut self) -> Poll<(), io::Error> {
        // Loop as long as the upstream is ready
        loop {
            // Get a reference to the buffer.
            let buf = match self.wr {
                Write::Head { ref mut head, .. } => head,
                _ => unreachable!(),
            };

            // If there is no more data to write, then the frame head has been
            // fully written, so return Ok.
            if !MutBuf::has_remaining(buf) {
                return Ok(Async::Ready(()));
            };

            // Write the data to the upstream. In the write case, 0 does not
            // mean that the upstream has shutdown, so there is no need to
            // check.
            let n = try_ready!(self.upstream.try_write_buf(buf));

            trace!("   --> wrote-head={}", n);
        }
    }

    // Write a frame payload. This function will be called as part of
    // `FramedIo::flush`. This fn is very similar to `write_head`
    fn write_data(&mut self) -> Poll<(), io::Error> {
        // Loop as long as the upstream is ready
        loop {
            // Get a reference to the buffer.
            let buf = match self.wr {
                Write::Data(ref mut buf) => buf,
                _ => unreachable!(),
            };

            // If there is no more data to write, then the frame payload has been
            // fully written, so return Okl
            if !MutBuf::has_remaining(buf) {
                return Ok(Async::Ready(()));
            };

            // Write the data to the upstream. In the write case, 0 does not
            // mean that the upstream has shutdown, so there is no need to
            // check.
            let n = try_ready!(self.upstream.try_write_buf(buf));

            trace!("   --> wrote-data={}", n);
        }
    }
}

// Implementation of `FramedIo` for `LengthDelimited`
impl<T: Io> FramedIo for LengthDelimited<T> {

    // Frames written to the socket are vectors of bytes. `LengthDelimited`
    // will first write the frame head, containing a 4-byte integer
    // representing the length, followed by writing the payload.
    type In = Vec<u8>;

    // Frame read from the socket. The head is not included. `None` indicates a
    // clean shutdown of the upstream
    type Out = Option<Vec<u8>>;

    // `LengthDelimited` is ready to read from when the upstream is ready
    fn poll_read(&mut self) -> Async<()> {
        self.upstream.poll_read()
    }

    fn read(&mut self) -> Poll<Self::Out, io::Error> {
        loop {
            match self.rd {
                Read::Head(..) => {
                    trace!("   --> reading head");
                    match try_ready!(self.read_head()) {
                        Some(n) => self.rd = Read::data(n),
                        None => return Ok(Async::Ready(None)),
                    }
                }
                Read::Data(..) => {
                    trace!("   --> reading data");
                    return self.read_data();
                }
            }
        }
    }

    // `LengthDelimited` is ready to write to when in the `Ready` state.
    //
    // This is because the write does not immediately write to the upstream,
    // but is instead buffered internally. So, to be write ready means to be
    // able to buffer the write.
    //
    // The actual write happens in flush.
    fn poll_write(&mut self) -> Async<()> {
        match self.wr {
            Write::Ready => Async::Ready(()),
            _ => Async::NotReady,
        }
    }

    fn write(&mut self, val: Vec<u8>) -> Poll<(), io::Error> {
        trace!("   --> in-payload-len={}", val.len());

        // Ensure that `self.wr` is currently in the ready state. If it is not,
        // then there is nothing to do with the value.
        assert!(self.poll_write().is_ready());

        // Serialize the payload length
        let mut head = [0; 4];
        NetworkEndian::write_u32(&mut head, val.len() as u32);

        // Transition `self.wr` to writing the head.
        self.wr = Write::Head { head: Cursor::new(head), data: val };

        Ok(Async::Ready(()))
    }

    // Attempt to write any buffered up data to the upstream
    fn flush(&mut self) -> Poll<(), io::Error> {
        // Once again, operate in a loop as long as the upstream is ready to
        // process more data.
        loop {
            match self.wr {
                // If in the ready state, then all data has been fully flushed
                // and there is nothing more to do
                Write::Ready => return Ok(Async::Ready(())),

                // Currently writing the frame head
                Write::Head { .. } => {
                    trace!("   --> writing head");

                    // Write the frame head, returning if `write_head` returns
                    // an error or `NotReady`
                    try_ready!(self.write_head());

                    // The head has been fully written to the upstream, transition to
                    // writing the payload
                    match mem::replace(&mut self.wr, Write::Ready) {
                        Write::Head { data, .. } => {
                            self.wr = Write::Data(Cursor::new(data));
                        }
                        _ => unreachable!(),
                    }
                }

                // Currently writing the frame payload
                Write::Data(..) => {
                    trace!("   --> writing data");

                    // Write the frame payload, returning if `write_data` returns
                    // an error or `NotReady`
                    try_ready!(self.write_data());

                    // The payload has been fully written to the upstream,
                    // transition to ready.
                    self.wr = Write::Ready;
                }
            }
        }
    }
}

impl Read {
    // Read head state with empty 4-byte buffer
    fn head() -> Read {
        Read::Head(allocate(4))
    }

    // Read payload state with empty `n` byte buffer
    fn data(n: usize) -> Read {
        Read::Data(allocate(n))
    }
}

fn allocate(n: usize) -> Take<Vec<u8>> {
    let v = Vec::with_capacity(n);
    assert_eq!(n, v.capacity());
    v.take(n)
}
