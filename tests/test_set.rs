extern crate minidb;
extern crate rand;
extern crate quickcheck;

#[macro_use]
extern crate log;
extern crate env_logger;

mod support;

use minidb::dt::*;
use support::*;

#[test]
fn simple_insert_remove() {
    let mut set: Set<String> = Set::new();
    let a = ActorId::from(0);

    set.insert(a, "hello");

    assert!(set.contains("hello"));
    assert!(!set.contains("world"));

    assert_eq!(1, set.len());

    set.remove(a, "world");
    assert_eq!(1, set.len());
    assert!(set.contains("hello"));

    set.remove(a, "hello");
    assert_eq!(0, set.len());
    assert!(!set.contains("hello"));
}

#[test]
fn join_two_isolated_sets() {
    let mut set1: Set<String> = Set::new();
    let mut set2: Set<String> = Set::new();

    let a = ActorId::from(0);
    let b = ActorId::from(1);

    set1.insert(a, "hello");
    set2.insert(b, "world");

    assert!(set1 != set2);

    set1.join(&set2);

    assert!(set1.contains("hello"));
    assert!(set1.contains("world"));

    // Joining again is idempotent
    let set1_2 = set1.clone();
    set1.join(&set2);

    assert_eq!(set1, set1_2);

    // Removing an element and joining again doesn't bring it back
    set1.remove(a, "world");
    set1.join(&set2);

    assert!(!set1.contains("world"));

    // Joining set 1 into 2 removes world
    set2.join(&set1);

    assert!(!set2.contains("world"));
}


#[test]
fn concurrent_insert_remove() {
    let mut set1: Set<String> = Set::new();
    let mut set2: Set<String> = Set::new();

    let a = ActorId::from(0);
    let b = ActorId::from(1);

    set1.insert(a, "hello");
    set2.insert(b, "hello");

    set2.join(&set1);
    assert!(set2.contains("hello"));

    set1.remove(a, "hello");
    assert!(!set1.contains("hello"));

    set2.join(&set1);
    assert!(set2.contains("hello"));

    set1.join(&set2);
    assert!(set1.contains("hello"));
}

/*
 *
 * ===== Fuzzing =====
 *
 */

#[test]
fn short_fuzz() {
    Fuzz::new()
        .num_iterations(3)
        .num_replicas(2)
        .join_weight(2)
        .random_factor(0)
        .run();
}

#[test]
fn longer_fuzz() {
    Fuzz::new()
        .num_iterations(20)
        .num_replicas(2)
        .join_weight(2)
        .random_factor(0)
        .run();
}

#[test]
fn another_fuzz() {
    Fuzz::new()
        .num_iterations(2)
        .num_replicas(2)
        .join_weight(3)
        .random_factor(27)
        .run();
}

#[test]
fn quickcheck_set() {
    use quickcheck::{QuickCheck, StdGen};
    use rand;

    fn prop(fuzz: Fuzz) -> bool {
        fuzz.try_run()
    }

    let _ = ::env_logger::init();

    QuickCheck::new()
        .gen(StdGen::new(rand::thread_rng(), 30))
        .tests(500)
        .max_tests(10_000)
        .quickcheck(prop as fn(Fuzz) -> bool);
}
