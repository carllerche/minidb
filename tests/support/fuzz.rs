extern crate rand;
extern crate quickcheck;

use minidb::dt::*;
use self::quickcheck::{Arbitrary, Gen};
use self::rand::{Rng, StdRng};

use std::cell::RefCell;

#[derive(Debug, Clone, Hash)]
pub struct Fuzz {
    // Probability of a join vs. a mutation
    join_weight: u32,
    // Number of op applications to make
    num_iterations: usize,
    // Number of replicas
    num_replicas: usize,
    // Extra number to randomize runs
    rand: u64,
}

struct Crdt {
    set: Set<String>,
    actor_id: ActorId,
}

// Some words to insert into the set
const WORDS: &'static [&'static str] = &[
    "foo", "bar", "baz", "qux",
    "one", "two", "three", "four", "five",
    "a", "b", "c", "d", "e", "f", "g",
];

impl Fuzz {
    pub fn new() -> Fuzz {
        Fuzz {
            join_weight: 1,
            num_iterations: 1,
            num_replicas: 1,
            rand: 0,
        }
    }

    pub fn num_iterations(mut self, val: usize) -> Self {
        self.num_iterations = val;
        self
    }

    pub fn num_replicas(mut self, val: usize) -> Self {
        self.num_replicas = val;
        self
    }

    pub fn join_weight(mut self, val: u32) -> Self {
        self.join_weight = val;
        self
    }

    pub fn random_factor(mut self, val: u64) -> Self {
        self.rand = val;
        self
    }

    pub fn run(&self) {
        assert!(self.try_run());
    }

    pub fn try_run(&self) -> bool {
        let _ = ::env_logger::init();

        assert!(self.num_replicas > 0);
        assert!(self.num_iterations > 0);

        let crdts = self.empty_values();

        let mut rng = self.rng();
        let mut rem = self.num_iterations;

        trace!("beginning fuzzing");

        while rem > 0 {
            // Pick a CRDT to operate on
            let i = rng.gen::<usize>() % crdts.len();
            let mut curr = crdts[i].borrow_mut();

            if rng.gen_weighted_bool(self.join_weight) {
                rem -= 1;

                // Mutate the value
                trace!("mutate; idx={:?}", i);
                self.mutate(&mut *curr, &mut rng);
                // self.harness.mutate(&mut *curr, &mut rng);
            } else {
                let j = self.other(i, crdts.len(), &mut rng);

                trace!("join; src={:?}; dst={:?}", j, i);
                self.join(&mut *curr, &*crdts[j].borrow());
            }
        }

        // Now it is time to ensure that we converge...
        self.converge(&crdts, &mut rng);

        trace!("checking sets");

        for i in 1..crdts.len() {
            // Check that both values are equal
            if crdts[0].borrow().set != crdts[i].borrow().set {
                assert_eq!(crdts[0].borrow().set.version_vec(), crdts[i].borrow().set.version_vec());
                // crdts[0].borrow().set.hax(&crdts[i].borrow().set);

                debug!("failed to match {}", i);
                trace!("  -> Base: {:?}", crdts[0].borrow().set);
                trace!("");
                trace!("  -> Other: {:?}", crdts[i].borrow().set);

                return false;
            }
        }

        true
    }

    fn converge(&self, crdts: &[RefCell<Crdt>], rng: &mut StdRng) {
        use std::cmp::Ordering::*;

        trace!(" ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ");
        trace!("converging...");
        trace!(" ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ");

        let mut others = vec![];

        loop {
            let mut max = 0;
            let mut done = true;

            assert!(crdts.len() > 1);

            for i in 1..crdts.len() {
                let val_1 = crdts[i].borrow();
                let val_2 = crdts[max].borrow();

                match val_1.set.version_vec().partial_cmp(&val_2.set.version_vec()) {
                    Some(Greater) => {
                        done = false;
                        max = i;
                    }
                    Some(Equal) => {}
                    _ => {
                        done = false;
                    }
                }
            }

            if done {
                trace!("done converging.");
                return;
            }

            let curr = crdts[max].borrow();

            // Create a list of CRDTs that have more info
            others.clear();

            for j in 0..crdts.len() {
                if j != max {
                    if !crdts[j].borrow().set.version_vec().ge(&curr.set.version_vec()) {
                        others.push(j);
                    }
                }
            }

            assert!(!others.is_empty(), "WAT");
            let j = *rng.choose(&others).unwrap();

            let before = crdts[j].borrow().set.version_vec().clone();

            trace!("join; src={:?}; dst={:?}; cmp={:?}", max, j, before.partial_cmp(&curr.set.version_vec()));
            self.join(&mut *crdts[j].borrow_mut(), &*curr);

            let after = crdts[j].borrow().set.version_vec().clone();

            assert!(after > before, "before={:?};\n\nafter={:?}", before, after);
        }
    }

    fn empty_values(&self) -> Vec<RefCell<Crdt>> {
        (0..self.num_replicas)
            .map(|i| {
                let crdt = Crdt {
                    set: Set::new(),
                    actor_id: (i as u64).into(),
                };

                RefCell::new(crdt)
            })
            .collect()
    }

    /// Mutates the given `Root` tracking the generated `Delta`
    fn mutate(&self, crdt: &mut Crdt, rng: &mut StdRng) {
        for _ in 0..(1 + rng.gen::<usize>() % 2) {
            let word = WORDS[rng.gen::<usize>() % WORDS.len()];

            if rng.gen_weighted_bool(15) {
                // Try to rarely get clear
                trace!("clear");
                crdt.set.clear(crdt.actor_id);
            } else if rng.gen_weighted_bool(3) {
                // Weigh removes a bit less
                trace!("remove; word={:?}", word);
                crdt.set.remove(crdt.actor_id, word)
            } else {
                trace!("insert; word={:?}", word);
                crdt.set.insert(crdt.actor_id, word)
            }
        }
    }

    /// Joins the given `Root` with the other
    fn join(&self, crdt: &mut Crdt, other: &Crdt) {
        crdt.set.join(&other.set);
    }

    /// Deterministically create a random number generator
    fn rng(&self) -> StdRng {
        use std::hash::{Hash, Hasher, SipHasher};
        use self::rand::SeedableRng;

        let mut s = SipHasher::new();
        self.hash(&mut s);
        let res = s.finish();

        StdRng::from_seed(&[res as usize])
    }

    fn other(&self, curr: usize, len: usize, rng: &mut StdRng) -> usize {
        assert!(len > 1);

        let mut j = rng.gen::<usize>() % (len - 1);

        if j >= curr {
            j += 1;
        }

        j
    }
}

impl Arbitrary for Fuzz {
    fn arbitrary<G: Gen>(g: &mut G) -> Fuzz {
        let num_replicas = usize::arbitrary(g) + 2;

        Fuzz {
            join_weight: (u32::arbitrary(g) % (num_replicas as u32) * 2) + 1,
            // Number of op applications to make
            num_iterations: usize::arbitrary(g) + 1,
            // Number of replicas
            num_replicas: num_replicas,
            // Extra number to randomize runs
            rand: g.gen(),
        }
    }

    fn shrink(&self) -> Box<Iterator<Item=Fuzz>> {
        let rand = self.rand;
        let join_weight = self.join_weight;

        let i = (self.num_iterations, self.num_replicas).shrink()
            .filter(|&(a, b)| a > 0 && b > 1)
            .map(move |(num_iterations, num_replicas)| {
                Fuzz {
                    join_weight: join_weight,
                    num_iterations: num_iterations,
                    num_replicas: num_replicas,
                    rand: rand,
                }
            });

        Box::new(i)
    }
}
