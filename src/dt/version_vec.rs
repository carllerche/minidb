use super::{ActorId, Timestamp, Dot};

use std::ops;
use std::cmp::Ordering;
use std::collections::HashMap;

/// A mechanism for tracking changes in a distributed system.
///
/// # Overview
///
/// A [version vector](https://en.wikipedia.org/wiki/Version_vector) is a
/// mechanism for tracking changes to data in a distributed system, where
/// multiple agents might update the data at different times. Version vectors
/// are partially ordered. Two version vectors may be compared to determine if
/// one represents a state that causally preceeds the other (happens-before) or
/// if the two version vectors represent state that is concurrent.
///
/// # Implementation
///
/// `VersionVec` is implemented as a `HashMap` of `ActorId` to `Timestamp`. For
/// each associated `ActorId` key, a `VersionVec` implies knowing about all
/// events for that `ActorId` that happend up to the associated `Timestamp`.
///
/// So, if `VersionVec` contains `ActorId(1) => Timestamp(4)`, the `VersionVec`
/// implies knowledge of events at timestamp 1 through 4 (inclusive) for
/// `ActorId(1)`.
///
/// An alternate implementation could be to represent a `VersionVec` as a
/// `HashSet<Dot>` such that the set contains the associated `Dot` for each
/// event that the `VersionVec` knows about, however since in MiniDB there are
/// never any "gaps" of knowledge for a single `ActorId`, using the `HashMap`
/// implementation is more efficient.
///
/// There are other CRDT, notably [delta-mutation
/// CRDTs](https://arxiv.org/abs/1410.2803) which require tracking "gaps" in
/// the version vector.
///
/// # Ordering
///
/// Given two version vectors A and B. Comparison is defined as follows:
///
/// ### Equality
///
/// A and B are equal if A and B both contain the same actor ID entries
/// with the same associated timestamp values.
///
/// Entries with `Timestamp(0)` are equivalent to no entry.
///
/// The following are examples of equal version vectors:
///
/// ```text
/// A: {}
/// B: {}
///
/// A: {}
/// B: { ActorId(0) => Timestamp(0) }
///
/// A: { ActorId(0) => Timestamp(3) }
/// B: { ActorId(0) => Timestamp(3) }
///
/// A: { ActorId(0) => Timestamp(3), ActorId(1) => Timestamp(0) }
/// B: { ActorId(0) => Timestamp(3) }
/// ```
///
/// ### Greater than
///
/// A is greater than B for all entries in B, there are entries in A for
/// the same actor ID such that the timestamp in A is greater than or
/// equal to the timestamp in B. There also must be at least one actor
/// ID entry in A that is strictly greater than the one in B.
///
/// The following are examples of A strictly greater than B
///
/// ```text
/// A: { ActorId(0) => Timestamp(1) }
/// B: {}
///
/// A: { ActorId(0) => Timestamp(3), ActorId(1) => Timestamp(4) }
/// B: { ActorId(0) => Timestamp(2), ActorId(1) => Timestamp(4) }
/// ```
///
/// # Less than
///
/// A is less than B if B is greater than A.
///
/// ### Concurrent
///
/// Two version vectors that do not satisfy any of the previous
/// conditions are concurrent.
///
/// The following are examples of concurrent version vectors.
///
/// ```text
/// A: { ActorId(0) => Timestamp(1) }
/// B: { ActorId(1) => Timestamp(1) }
///
/// A: { ActorId(0) => Timestamp(1), ActorId(1) => Timestamp(2) }
/// B: { ActorId(0) => Timestamp(2), ActorId(1) => Timestamp(1) }
/// ```
///
/// # Usage
///
/// `VersionVec` is primarily used for comparison, using
/// `PartialOrd::partial_cmp`.
///
/// A new `VersionVec` is created with `default`.
///
/// ```rust
/// use minidb::dt::{VersionVec, ActorId};
///
/// let mut vv1 = VersionVec::default();
///
/// vv1.increment(ActorId(0));
///
/// let mut vv2 = vv1.clone();
/// vv2.increment(ActorId(0));
///
/// assert!(vv1 < vv2);
///
/// ```
///
/// Comparing two concurrent version vectors.
///
/// ```rust
/// use minidb::dt::{VersionVec, ActorId};
///
/// let mut vv1 = VersionVec::default();
/// let mut vv2 = VersionVec::default();
///
/// vv1.increment(ActorId(0));
/// vv2.increment(ActorId(1));
///
/// assert!(vv1.partial_cmp(&vv2).is_none());
///
/// // Join the two version vectors into a third
/// let mut vv3 = vv1.clone();
/// vv3.join(&vv2);
///
/// assert!(vv1 < vv3);
/// assert!(vv2 < vv3);
/// ```
///
#[derive(Debug, Clone, Default, Eq)]
pub struct VersionVec {
    inner: HashMap<ActorId, Timestamp>,
}

impl VersionVec {

    /// Returns `true` if the `VersionVec` represents knowledge of the given `Dot`
    pub fn contains(&self, dot: Dot) -> bool {
        // Using `self[...]` here will never panic. If there is no entry for
        // the given actor ID, then `Timestamp(0)` is returned.
        self[dot.actor_id()] >= dot.timestamp()
    }

    /// Increment the timestamp associated with `actor_id`
    ///
    /// Incrementing the timestamp creates a new `Dot` representing a new event
    /// for the given `actor_id`. The `Dot` is returned and may be used as part
    /// of a `Set` operation.
    ///
    /// If the `VersionVec` does not currently have an associated timestamp for
    /// `actor_id` then the newly created `Dot` will have a timestamp of 1.
    pub fn increment(&mut self, actor_id: ActorId) -> Dot {
        // Get the timestamp for the given `actor_id`. If there is no entry for
        // the `actor_id`, create one using `Timestamp(0)`, aka
        // `Timestamp::default()`.
        let ts = self.inner.entry(actor_id)
            .or_insert(Timestamp::default());

        // Increment the timestamp
        ts.increment();

        // Return a `Dot` for the newly incremented timestamp. This `Dot` can
        // then be used to represent `Set` operations.
        Dot::new(actor_id, *ts)
    }

    /// Updates the current version vector to represent the union between
    /// `self` and `other.
    ///
    /// After the join operation, the version vector will be the [least upper
    /// bound](https://en.wikipedia.org/wiki/Least-upper-bound_property) of the
    /// original value and `other`.
    ///
    /// If the `VersionVec` were implemented as a set of all known dots, then a
    /// join would be the set union of the dots in `self` with `other.
    pub fn join(&mut self, other: &VersionVec) {

        // A join is perfomed by keeping the timestamp values for all actor ID
        // entries that only appear in one of the two version vectors. For
        // actor IDs that exist in both version vectors, the resulting
        // timestamp will be the max between the timestamp in &self and the
        // timestamp in `other.

        for (&actor_id, &other_ts) in &other.inner {
            let self_ts = self.inner.entry(actor_id)
                .or_insert(Timestamp::default());

            if other_ts > *self_ts {
                *self_ts = other_ts;
            }
        }
    }
}

impl ops::Index<ActorId> for VersionVec {
    type Output = Timestamp;

    fn index(&self, index: ActorId) -> &Timestamp {
        pub static ZERO: Timestamp = Timestamp(0);

        // Return the `Timestamp` associated with `index`. If there is no
        // `Timestamp` associated with `index`, return `Timestamp(0)`.
        self.inner.get(&index)
            .unwrap_or(&ZERO)
    }
}

impl PartialEq for VersionVec {
    fn eq(&self, other: &VersionVec) -> bool {
        self.partial_cmp(other) == Some(Ordering::Equal)
    }
}

impl PartialOrd for VersionVec {
    fn partial_cmp(&self, other: &VersionVec) -> Option<Ordering> {
        use std::cmp::Ordering::*;

        // Start off by assuming that the two version vectors are Equal. This
        // will handle the case that neither version vectors contain any
        // entries.
        let mut ret = Equal;

        // For each entry in `&self`, check the timestamp in `other` for the
        // same `ActorId`.
        for (&actor_id, &timestamp) in &self.inner {

            // Compare both timestamps, by using `other[actor_id]`, if `other`
            // does not contain an entry for `actor_id`, `Timestamp(0)` is
            // returned.
            match (ret, timestamp.cmp(&other[actor_id])) {
                // If both entries are equal, then the in-progress comparison
                // result is not alterned.
                //
                // If `ret` == `Less`, then continue assuming `Less`
                // If `ret` == `Greater`, then continue assuming `Greater`
                // if `ret` == `Equal`, then continue assuming `Equal`,
                (_, Equal) => {},
                // All prior entries have been less than or equal to `&other`,
                // continue, the current result is `Less`
                (Less, Less) => {},
                // All prior entries have been greater than or equal to
                // `&other`, continue, the current result is `Greater`
                (Greater, Greater) => {},
                // At this point, all previous entries have had equal Timestamp
                // values, so transition the overall result to the result of
                // the last comparison.
                (Equal, res) => ret = res,
                // All other options imply that both version vectors are
                // concurrent, so return `None` here to avoid performing
                // further work.
                _ => return None,
            }
        }

        // Now, iterate entries in the other version vec, looking for entries
        // that are not included in &self`
        for (&actor_id, &ts) in &other.inner {
            // Can skip explicit zero timestamps as those are equivalent to
            // missing.
            if ts == 0 {
                continue;
            }

            // If the value of the timestamp in &self is 0, aka, the `ActorId`
            // exists in `other` but not `self`, then &self can only be less
            // than `other` or concurrent.
            if self[actor_id] == 0 {
                match ret {
                    Equal | Less => return Some(Less),
                    _ => return None,
                }
            }
        }

        // Return the comparison result
        Some(ret)
    }
}

mod proto {
    use dt::Dot;
    use super::*;
    use buffoon::*;
    use std::io;
    use std::collections::HashMap;

    impl Serialize for VersionVec {
        fn serialize<O: OutputStream>(&self, out: &mut O) -> io::Result<()> {
            // Serialize the version vec as a series of dots
            let dots = self.inner.iter()
                .filter(|&(_, &ts)| ts != 0)
                .map(|(actor_id, ts)| Dot::new(*actor_id, *ts));

            try!(out.write_repeated(1, dots));
            Ok(())
        }
    }

    impl Deserialize for VersionVec {
        fn deserialize<R: io::Read>(i: &mut InputStream<R>) -> io::Result<VersionVec> {
            let mut inner = HashMap::new();

            while let Some(f) = try!(i.read_field()) {
                match f.tag() {
                    1 => {
                        let dot: Dot = try!(f.read());
                        assert!(inner.insert(dot.actor_id(), dot.timestamp()).is_none());
                    }
                    _ => try!(f.skip()),
                }
            }

            Ok(VersionVec { inner: inner })
        }
    }
}
