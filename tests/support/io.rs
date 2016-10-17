extern crate futures;
extern crate tokio_core;

use self::futures::*;
use self::futures::stream::Stream;
use self::tokio_core::reactor::*;
use self::tokio_core::io::FramedIo;
use std::io;

// Convert a `FramedIo` into a read stream
struct IoStream<T> {
    io: T,
}

impl<T, U> Stream for IoStream<T>
    where T: FramedIo<Out = Option<U>>
{
    type Item = U;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<U>, io::Error> {
        self.io.read()
    }
}

pub fn collect_chunks<T, U>(io: T) -> io::Result<Vec<U>>
    where T: FramedIo<Out = Option<U>>
{
    let _ = ::env_logger::init();

    let mut ret = vec![];
    let mut core = Core::new().unwrap();

    let io_stream = IoStream { io: io };

    try!(core.run(io_stream.for_each(|chunk| {
        ret.push(chunk);
        Ok(())
    })));

    Ok(ret)
}
