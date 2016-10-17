//! MiniDB distributed data types
//!
//! The primary data structure is [Set](struct.Set.html), which is a CRDT set
//! implementation. The rest of the types in this module are in support of
//! `Set`.
//!
//! See documentation for each type for more information.

mod set;
mod version_vec;

pub use self::set::Set;
pub use self::version_vec::VersionVec;

/// Uniquely identifies a process in the MiniDB cluster.
///
/// `ActorId` is used as part of [VersionVec](struct.VersionVec.html) and
/// [Dot](struct.Dot.html) as well as passed as an argument to functions on
/// [Set](struct.Set.html).
///
/// It is **critical** that `ActorId` values are not reused across processes.
/// Since MiniDB is an in-memory only database and all state is lost when the
/// process shuts down, the `ActorId` is randomly generated on start. This
/// should (hopefully) ensure that there is no collision.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct ActorId(pub u64);

/// A **logical** timestamp associated with events within a single process.
///
/// It is assumed that events within a single process are totally ordered.
/// As such, tracking the order of each event can be done with a single numeric
/// value. Each event is assigned the next numeric value and event ordering is
/// done by ordering the associated timestamp.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Timestamp(u64);

/// Represents a discrete event.
///
/// `Dot` is equivalent to `(ActorId, Timestamp)`. Since every event in a
/// MiniDB cluster takes place on a node in the cluster at a single point in
/// time, all events are uniquely identified by `Dot`.
///
/// On each `Set` event, a MiniDB node will generate a new `Dot` to represent
/// this event. This is done by incrementing the `Timestamp` counter and using
/// that value with the node's `ActorId`.
///
/// `Dot` only represents that specific event with no implication of prior
/// causality. In other words, if a process knows of dot `(ActorId(1),
/// Timestamp(2))`, there is no implication that the same process knows of
/// `(ActorId(1), Timestamp(1))`
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct Dot {
    actor_id: ActorId,
    timestamp: Timestamp,
}

/*
 *
 * ===== impl ActorId =====
 *
 */

impl From<u64> for ActorId {
    fn from(src: u64) -> ActorId {
        ActorId(src)
    }
}

/*
 *
 * ===== impl Timestamp =====
 *
 */

impl Timestamp {
    /// Increment the timestamp
    ///
    /// This increments the internal counter by one.
    pub fn increment(&mut self) {
        self.0 += 1;
    }
}

// Define a default `Timestamp` value to be 0. A 0 value associated with an
// ActorId is equivalent to an ActorId not having any timestamp associated with
// it.
impl Default for Timestamp {
    fn default() -> Timestamp {
        Timestamp(0)
    }
}

impl PartialEq<u64> for Timestamp {
    fn eq(&self, other: &u64) -> bool {
        self.0.eq(other)
    }
}

/*
 *
 * ===== impl Dot =====
 *
 */

impl Dot {
    /// Return a new `Dot` for the given `ActorId` and `Timestamp`
    pub fn new(actor_id: ActorId, timestamp: Timestamp) -> Dot {
        Dot {
            actor_id: actor_id,
            timestamp: timestamp,
        }
    }

    /// Returns a reference to the `ActorId` associated with the `Dot`
    pub fn actor_id(&self) -> ActorId {
        self.actor_id
    }

    /// Returns a reference to the `Timestamp` associated with the `Dot`
    pub fn timestamp(&self) -> Timestamp {
        self.timestamp
    }
}

mod proto {
    //! Define protobuf serialization / deserialization for the data types in
    //! `minidb::dt`. This is done with the `buffoon` library.

    use super::*;
    use buffoon::*;
    use std::io;

    /*
     *
     * ===== ActorId =====
     *
     */

    impl Serialize for ActorId {
        fn serialize<O: OutputStream>(&self, out: &mut O) -> io::Result<()> {
            out.write(1, &self.0)
        }
    }

    impl Deserialize for ActorId {
        fn deserialize<R: io::Read>(i: &mut InputStream<R>) -> io::Result<ActorId> {
            let mut id = None;

            while let Some(f) = try!(i.read_field()) {
                match f.tag() {
                    1 => id = Some(try!(f.read())),
                    _ => try!(f.skip()),
                }
            }

            Ok(ActorId(required!(id)))
        }
    }

    /*
     *
     * ===== Timestamp =====
     *
     */

    impl Serialize for Timestamp {
        fn serialize<O: OutputStream>(&self, out: &mut O) -> io::Result<()> {
            out.write(1, &self.0)
        }
    }

    impl Deserialize for Timestamp {
        fn deserialize<R: io::Read>(i: &mut InputStream<R>) -> io::Result<Timestamp> {
            let mut ts = None;

            while let Some(f) = try!(i.read_field()) {
                match f.tag() {
                    1 => ts = Some(try!(f.read())),
                    _ => try!(f.skip()),
                }
            }

            Ok(Timestamp(required!(ts)))
        }
    }

    /*
     *
     * ===== Dot =====
     *
     */

    impl Serialize for Dot {
        fn serialize<O: OutputStream>(&self, out: &mut O) -> io::Result<()> {
            try!(out.write(1, &self.actor_id()));
            try!(out.write(2, &self.timestamp()));
            Ok(())
        }
    }

    impl Deserialize for Dot {
        fn deserialize<R: io::Read>(i: &mut InputStream<R>) -> io::Result<Dot> {
            let mut actor_id = None;
            let mut timestamp = None;

            while let Some(f) = try!(i.read_field()) {
                match f.tag() {
                    1 => actor_id = Some(try!(f.read())),
                    2 => timestamp = Some(try!(f.read())),
                    _ => try!(f.skip()),
                }
            }

            Ok(Dot::new(
                    required!(actor_id),
                    required!(timestamp)))
        }
    }
}
