use config;

use tokio_core::io::FramedIo;
use tokio_timer::{Timer, Sleep};

use futures::{Future, Async, Poll};
use std::io;
use std::collections::VecDeque;
use std::time::Duration;

/// Junkify turns the network connection into rubbish.
///
/// A distributed database isn't much fun if it doesn't properly handle a
/// glitchy network. Testing this locally is hard. `Junkify` enables simulating
/// a slow or partitioned network.
///
/// `Junkify` is created with an upstream `FramedIo` as well as a
/// `config::Route` which informs it out to great a packet being written to the
/// socket.
///
/// `Junkify` can support random delays in outbound packets or it can drop the
/// packet, ensuring that the remote will never see it.
pub struct Junkify<T: FramedIo> {
    upstream: T,
    route: Option<config::Route>,
    timer: Option<Timer>,
    queue: VecDeque<T::In>,
    sleep: Option<Sleep>,
}

impl<T: FramedIo> Junkify<T> {
    pub fn new(upstream: T, route: Option<&config::Route>, timer: Option<&Timer>) -> Junkify<T> {
        assert_eq!(route.is_some(), timer.is_some());

        Junkify {
            upstream: upstream,
            route: route.map(Clone::clone),
            timer: timer.map(Clone::clone),
            queue: VecDeque::new(),
            sleep: None,
        }
    }
}

impl<T: FramedIo> FramedIo for Junkify<T>
    where T: FramedIo,
{
    type In = T::In;
    type Out = T::Out;

    fn poll_read(&mut self) -> Async<()> {
        self.upstream.poll_read()
    }

    fn read(&mut self) -> Poll<Self::Out, io::Error> {
        self.upstream.read()
    }

    fn poll_write(&mut self) -> Async<()> {
        self.upstream.poll_write()
    }

    fn write(&mut self, val: Self::In) -> Poll<(), io::Error> {
        use config::Action::*;

        match self.route {
            Some(ref route) => {
                match route.action() {
                    Allow => {
                        self.queue.push_back(val);
                        Ok(Async::Ready(()))
                    }
                    Delay => {
                        if self.sleep.is_none() {
                            let dur = rand_duration();
                            info!("delaying write to {:?} for {:?}", self.route.as_ref().unwrap().destination(), dur);
                            let sleep = self.timer.as_mut().unwrap().sleep(dur);
                            self.sleep = Some(sleep);
                        }

                        self.queue.push_back(val);
                        Ok(Async::Ready(()))
                    }
                    Deny => {
                        // Drop the data
                        Ok(Async::Ready(()))
                    }
                }
            }
            None => self.upstream.write(val),
        }
    }

    fn flush(&mut self) -> Poll<(), io::Error> {
        use config::Action::*;

        while !self.queue.is_empty() {
            if let Some(ref mut sleep) = self.sleep {
                try_ready!(sleep.poll());
                info!("writing delayed write to {:?}", self.route.as_ref().unwrap().destination());
            }

            self.sleep = None;

            try!(self.upstream.flush());

            if !self.upstream.poll_write().is_ready() {
                break;
            }

            let chunk = self.queue.pop_front().unwrap();
            try!(self.upstream.write(chunk));

            if !self.queue.is_empty() {
                if let Delay = self.route.as_ref().unwrap().action() {
                    let dur = rand_duration();
                    info!("delaying write to {:?} for {:?}", self.route.as_ref().unwrap().destination(), dur);
                    let sleep = self.timer.as_mut().unwrap().sleep(dur);
                    self.sleep = Some(sleep);
                }
            }
        }

        self.upstream.flush()
    }
}

fn rand_duration() -> Duration {
    use rand::{self, Rng};

    let mut rng = rand::thread_rng();
    let secs = rng.next_u64() % 7;
    Duration::from_secs(secs)
}
