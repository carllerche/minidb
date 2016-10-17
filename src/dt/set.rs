use super::{VersionVec, ActorId, Dot};

use std::mem;
use std::borrow::Borrow;
use std::collections::{HashSet, HashMap};
use std::collections::hash_map::Keys;
use std::hash::Hash;

/// A CRDT Set implementation providing add-wins semantics
///
/// # Overview
///
/// An implementation of a
/// [CRDT](https://en.wikipedia.org/wiki/Conflict-free_replicated_data_type)
/// set. This implementation is specifically the "Add-wins optimized
/// observed-remove set".
///
/// The Set is "add-wins", which means that if an insertion operation and a
/// removal operation for the same value happen concurrently, the value will
/// exist in the result of a join. Another way to think about it, is that the
/// remove operation is only able to operate on insertions that
/// [happened-before](https://en.wikipedia.org/wiki/Happened-before) the
/// removal.
///
/// It is an optimized implementation due to the fact that the removal
/// operation does not require a tombstone.
///
/// The set provides the following operations:
///
/// * `insert`: Insert a value into the set.
/// * `remove`: Remove a value from the set.
/// * `causal_remove`: Remove a value from the set that is known to have been
///    inserted into the set at a point represented by the provided version
///    vector.
/// * `clear`: Clear all values from the set
///
/// # Implementation
///
/// On insertion, a dot is generated to represent the event. The set stores all
/// values with dots representing their insertion. Since the same value can be
/// concurrently inserted, there may be more than one dot per value.
///
/// The set also contains a version vector. The dot generated on insertion is
/// also tracked in this version vector.
///
/// Removing a value is done by clearing all of its associated dots. However,
/// the dots are still represented in the version vector.
///
/// Joining two sets involves finding all elements that exist in both sets or
/// elements that exist in one set which have dots that are not contained by
/// the other set's version vector.
///
/// If a value in one set does not exist in the other set, but the dot is
/// contained by the other version vector, this means that the other set has
/// already seen the insertion and has removed the value.
///
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Set<T: Hash + Eq + Clone> {
    version_vec: VersionVec,
    elements: HashMap<T, HashSet<Dot>>,
}

pub struct Iter<'a, T: 'a + Hash + Eq + Clone> {
    inner: Keys<'a, T, HashSet<Dot>>,
}

impl<T: Hash + Eq + Clone> Set<T> {

    /// Returns a new empty `Set`
    ///
    pub fn new() -> Set<T> {
        Set {
            version_vec: VersionVec::default(),
            elements: HashMap::new(),
        }
    }

    /// Returns a reference to the version vector
    pub fn version_vec(&self) -> &VersionVec {
        &self.version_vec
    }

    /// Returns true if the `Set` contains the specified value.
    ///
    pub fn contains<Q: ?Sized>(&self, val: &Q) -> bool
        where T: Borrow<Q>, Q: Hash + Eq
    {
        // If the `elements` structure has an entry with key `val` then the
        // CRDT set is considered to contain the value.
        //
        // The assumption is that there is at least one dot associated with the
        // value. Any operation which leaves an `elements` key with no
        // remaining dots should also remove the entry from the `elements` hash
        // map.
        match self.elements.get(val) {
            Some(dots) => {
                debug_assert!(!dots.is_empty(), "expected at least one dot associated with the value");
                true
            }
            None => false,
        }
    }

    /// Returns the number of elements in the `Set`.
    ///
    pub fn len(&self) -> usize {
        // The number of entries in the `elements` HashMap is equivalent to the
        // number of elements in the set CRDT. There are CRDT set
        // implementations for which this is not true, for example, sets where
        // removals are tracked with tombstones.
        self.elements.len()
    }

    /// Returns true if the `Set` contains no elements
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Insert a value into the set
    ///
    /// A `Dot` is generated for the given `actor_id`. This dot is used to
    /// track the insertion across joins. The value is then inserted into the
    /// set.
    pub fn insert<U>(&mut self, actor_id: ActorId, value: U)
        where U: Into<T>
    {
        // Get a reference to the dot set for the value. If there is no entry
        // for the value, insert an empty `HashSet`
        let dots = self.elements.entry(value.into())
            .or_insert(HashSet::new());

        // Generate a new dot and update the version vector with the dot
        let dot = self.version_vec.increment(actor_id);

        // First, remove all the dots associated with the element. This is an
        // optimization which helps reduce the amount of needed storage for the
        // set.
        //
        // Since all existing dots for the `value` "happened before" this
        // insertion, the newly generated dot is able to represent all of these
        // dots as well. So, tracking the old dots is not necessary.
        dots.clear();

        // Insert the dot into the dot set for the value
        dots.insert(dot);
    }

    /// Removes a value from the set.
    pub fn remove<Q: ?Sized>(&mut self, actor_id: ActorId, value: &Q)
        where T: Borrow<Q>, Q: Hash + Eq
    {
        // Create & track a dot representing the removal operation
        //
        // Generating a dot for removals is not strictly necessary to implement
        // the set CRDT (none of the literature does so). However, generating a
        // dot allows using the version vectors of two sets to determine if
        // they are "in sync".
        //
        // Imagine two identical sets, A and B. A value is removed from A.
        // Since a dot was generated, A.version_vec() > B.version_vec(). We now
        // know that B is out of date and must be synchronized.
        self.version_vec.increment(actor_id);

        // Removing an element from the set implies clearing all associated
        // dots. This is done by removing the entry from the `elements`
        // HashMap.
        self.elements.remove(value);
    }

    /// Removes a value from the set that is "present" at the provided version
    /// vector.
    ///
    /// This operation is useful for providing "atomic" removals. Given a
    /// client that wishes to read the set and remove a value that exists in
    /// the set, `causal_remove` enables doing this without accidentally
    /// removing a concurrent insert that was not visible in the original read.
    ///
    /// Using `causal_remove`, a client would get the set, extract the version
    /// vector from the response, and include the version vector in the remove
    /// operation.
    ///
    /// `cause_remove` works by disassociating dots from the value being
    /// removed only if those dots are contained by the provided version
    /// vector. This ensures that only dots that were known about by the client
    /// can be cleared. Any concurrent insertion will be represented by a dot
    /// that was not included in version vector provided to `causal_remove`.
    ///
    /// `remove` could be implemented as follows:
    ///
    /// ```rust,ignore
    /// let vv = self.vv.clone();
    /// self.causal_remove(actor_id, &vv, value);
    /// ```
    pub fn causal_remove<Q: ?Sized>(&mut self, actor_id: ActorId, vv: &VersionVec, value: &Q)
        where T: Borrow<Q>, Q: Hash + Eq
    {
        // First generate a dot representing the removal. See the comment in
        // `remove` for more detail.
        self.version_vec.increment(actor_id);

        // Create a new dot set representing the result of the remove.
        let mut next_dots = HashSet::new();

        if let Some(current_dots) = self.elements.get(value) {
            // Iterate all dots for `value`. If the dot does not exist in `vv`,
            // then insert the dot in `next_dots`.
            //
            // If a dot is not contained by `vv` then it was not "known about"
            // when the remove operation was issued and should remain in the
            // set.

            for &dot in current_dots {
                if !vv.contains(dot) {
                    next_dots.insert(dot);
                }
            }
        }

        // If there are no dots in `next_dots`, then the value is effectively
        // removed from the set, so remove the entry in `self.elements`.
        // Otherwise, update the entry with `next_dots`.
        if next_dots.is_empty() {
            self.elements.remove(value);
        } else {
            *self.elements.get_mut(value).unwrap() = next_dots;
        }
    }

    /// Clears the set, removing all values.
    pub fn clear(&mut self, actor_id: ActorId) {
        // This function is essentially removing all values from the set. The
        // implementation is an optimization over iterating all the values and
        // calling `remove` for each value.
        //
        // See comments in `remove` for more detail.

        self.version_vec.increment(actor_id);
        self.elements.clear();
    }

    /// An iterator visiting all elements in arbitrary order.
    pub fn iter(&self) -> Iter<T> {
        Iter { inner: self.elements.keys() }
    }

    /// Join the given set into the current one.
    ///
    /// The current set will then contain the combined state of the previous
    /// value and the `other` set.
    ///
    /// See module documentation for more details.
    pub fn join(&mut self, other: &Set<T>) {

        // Create a new HashMap which will contain the result of the join
        let mut joined_elements = HashMap::new();

        // Create an empty hash set. This is used later on as a default value
        // when the other set doesn't have an entry for the current value being
        // processed.
        let empty_dots = HashSet::new();

        // Iterate each element in the current set.
        //
        // This is done via `drain`, essentially consuming `elements`. This is
        // needed since Rust's HashMap doesn't provide an API for removing
        // elements while iterating.
        //
        // Elements that are expected to remain in the set are inserted into
        // `joined_elements`. At the end of the function, `self.elements` is
        // replaced with `joined_elements`
        for (elem, self_dots) in self.elements.drain() {

            // Create a new dot set which will contain the result of the join
            // for the current element
            let mut joined_dots = HashSet::new();

            // The other `Set` does not contain `elem`.  There are two
            // possible cases for this:
            //
            // 1) The other set has never seen `elem`.
            // 2) The other set has seen `elem` and has removed it.
            //
            // To distinguish between these two cases, the other set's
            // version vector is checked to see if it contains the dot.
            let other_dots = other.elements.get(&elem).unwrap_or(&empty_dots);

            for dot in self_dots {
                if other_dots.contains(&dot) {
                    // The dot is in both sets, so keep it in the join
                    joined_dots.insert(dot);
                } else if other.version_vec.contains(dot) {
                    // The other set has seen the dot and has removed it,
                    // don't join the dot
                } else {
                    // The other set has not yet seen this dot, so include it
                    // in the join
                    joined_dots.insert(dot);
                }
            }

            // Now iterate all dots in `other` for the current value. This is
            // needed in order to cover any dots in `other` that are not in
            // `&self`.
            for &dot in other_dots {

                // For each dot, it is simply necessary to check if `&self`'s
                // version vector does not contain the dot. All the other cases
                // are covered in the previous checks.
                if !self.version_vec.contains(dot) {

                    // `&self` has not seen the dot yet, track it for the
                    // join result.
                    joined_dots.insert(dot);
                }
            }

            // If there dots in the joined dot set, insert the element in the
            // joined element set, otherwise, the value is not present in the
            // result of the join.
            if !joined_dots.is_empty() {
                joined_elements.insert(elem, joined_dots);
            }
        }

        // Iterate the other set. This iteration is to catch any value in
        // `other` that is not currently in `&self`
        for (elem, dots) in &other.elements {

            // Only process the element if it is not currently in
            // `joined_elements`, aka it is a value that is not contained by
            // `&self`.
            //
            // Note, `joined_elements` is checked here because at this point
            // `self.elements` has been drained.
            if !joined_elements.contains_key(&elem) {
                let mut joined_dots = HashSet::new();

                for dot in dots {
                    // If the dot is not in `self.version_vec` then this is a
                    // value that has not yet been seen by `self`, so track it
                    // in the result of the join.
                    if !self.version_vec.contains(*dot) {
                        joined_dots.insert(*dot);
                    }
                }

                // If `joined_dots` is not empty, insert the value into the
                // join result. If `joined_dots` is empty, this implies that
                // `self` has already seen the value and has removed it.
                if !joined_dots.is_empty() {
                    joined_elements.insert(elem.clone(), joined_dots);
                }
            }
        }

        // Join the version vecs
        self.version_vec.join(&other.version_vec);

        // Finally, store the result of the join in `self.elements`.
        mem::replace(&mut self.elements, joined_elements);
    }
}

impl<'a, T: 'a + Hash + Eq + Clone> Iterator for Iter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<&'a T> {
        self.inner.next()
    }
}

mod proto {
    use dt::Dot;
    use super::*;
    use buffoon::*;
    use std::io;
    use std::collections::{HashMap, HashSet};

    struct EntryRef<'a> {
        v: &'a str,
        dots: &'a HashSet<Dot>,
    }

    struct Entry {
        v: String,
        dots: HashSet<Dot>,
    }

    impl Serialize for Set<String> {
        fn serialize<O: OutputStream>(&self, out: &mut O) -> io::Result<()> {
            let entries = self.elements.iter()
                .map(|(v, dots)| EntryRef { v: v, dots: dots });

            // Serialize entries
            try!(out.write(1, &self.version_vec));
            try!(out.write_repeated(2, entries));
            Ok(())
        }
    }

    impl<'a> Serialize for EntryRef<'a> {
        fn serialize<O: OutputStream>(&self, out: &mut O) -> io::Result<()> {
            try!(out.write(1, self.v));
            try!(out.write_repeated(2, self.dots));
            Ok(())
        }
    }

    impl Deserialize for Set<String> {
        fn deserialize<R: io::Read>(i: &mut InputStream<R>) -> io::Result<Set<String>> {
            let mut version_vec = None;
            let mut elements = HashMap::new();

            while let Some(f) = try!(i.read_field()) {
                match f.tag() {
                    1 => {
                        version_vec = Some(try!(f.read()));
                    }
                    2 => {
                        let entry: Entry = try!(f.read());
                        elements.insert(entry.v, entry.dots);
                    }
                    _ => try!(f.skip()),
                }
            }

            Ok(Set {
                version_vec: required!(version_vec),
                elements: elements,
            })
        }
    }

    impl Deserialize for Entry {
        fn deserialize<R: io::Read>(i: &mut InputStream<R>) -> io::Result<Entry> {
            let mut v = None;
            let mut dots = HashSet::new();

            while let Some(f) = try!(i.read_field()) {
                match f.tag() {
                    1 => v = Some(try!(f.read())),
                    2 => {
                        let dot: Dot = try!(f.read());
                        dots.insert(dot);
                    }
                    _ => try!(f.skip()),
                }
            }

            Ok(Entry {
                v: required!(v),
                dots: dots,
            })
        }
    }
}
