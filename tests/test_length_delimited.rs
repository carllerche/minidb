extern crate minidb;
extern crate fixture_io;

#[macro_use]
extern crate log;
extern crate env_logger;

mod support;

use minidb::proto::LengthDelimited;
use fixture_io::FixtureIo;
use std::time::Duration;

#[test]
pub fn empty_io_yields_nothing() {
    let io = FixtureIo::empty();
    let io = LengthDelimited::new(io);

    let chunks = support::collect_chunks(io).unwrap();
    assert_eq!(chunks, expect(&[]));
}

#[test]
pub fn single_frame_one_packet() {
    let io = FixtureIo::empty()
        .then_read(&b"\x00\x00\x00\x09abcdefghi"[..]);

    let io = LengthDelimited::new(io);

    let chunks = support::collect_chunks(io).unwrap();
    assert_eq!(chunks, expect(&[b"abcdefghi"]));
}

#[test]
pub fn single_multi_frame_one_packet() {
    let mut data: Vec<u8> = vec![];
    data.append(&mut bytes(b"\x00\x00\x00\x09abcdefghi"));
    data.append(&mut bytes(b"\x00\x00\x00\x03123"));
    data.append(&mut bytes(b"\x00\x00\x00\x0bhello world"));

    let io = FixtureIo::empty()
        .then_read(data);

    let io = LengthDelimited::new(io);

    let chunks = support::collect_chunks(io).unwrap();
    assert_eq!(chunks, expect(&[b"abcdefghi", b"123", b"hello world"]));
}

#[test]
pub fn single_frame_multi_packet() {
    let io = FixtureIo::empty()
        .then_read(&b"\x00\x00"[..])
        .then_read(&b"\x00\x09abc"[..])
        .then_read(&b"defghi"[..])
        ;

    let io = LengthDelimited::new(io);

    let chunks = support::collect_chunks(io).unwrap();
    assert_eq!(chunks, expect(&[b"abcdefghi"]));
}

#[test]
pub fn multi_frame_multi_packet() {
    let io = FixtureIo::empty()
        .then_read(&b"\x00\x00"[..])
        .then_read(&b"\x00\x09abc"[..])
        .then_read(&b"defghi"[..])
        .then_read(&b"\x00\x00\x00\x0312"[..])
        .then_read(&b"3\x00\x00\x00\x0bhello world"[..])
        ;

    let io = LengthDelimited::new(io);

    let chunks = support::collect_chunks(io).unwrap();
    assert_eq!(chunks, expect(&[b"abcdefghi", b"123", b"hello world"]));
}

#[test]
pub fn single_frame_multi_packet_wait() {
    let io = FixtureIo::empty()
        .then_read(&b"\x00\x00"[..])
        .then_wait(ms(50))
        .then_read(&b"\x00\x09abc"[..])
        .then_wait(ms(50))
        .then_read(&b"defghi"[..])
        .then_wait(ms(50))
        ;

    let io = LengthDelimited::new(io);

    let chunks = support::collect_chunks(io).unwrap();
    assert_eq!(chunks, expect(&[b"abcdefghi"]));
}

#[test]
pub fn multi_frame_multi_packet_wait() {
    let io = FixtureIo::empty()
        .then_read(&b"\x00\x00"[..])
        .then_wait(ms(50))
        .then_read(&b"\x00\x09abc"[..])
        .then_wait(ms(50))
        .then_read(&b"defghi"[..])
        .then_wait(ms(50))
        .then_read(&b"\x00\x00\x00\x0312"[..])
        .then_wait(ms(50))
        .then_read(&b"3\x00\x00\x00\x0bhello world"[..])
        .then_wait(ms(50))
        ;

    let io = LengthDelimited::new(io);

    let chunks = support::collect_chunks(io).unwrap();
    assert_eq!(chunks, expect(&[b"abcdefghi", b"123", b"hello world"]));
}

#[test]
pub fn incomplete_head() {
    let io = FixtureIo::empty()
        .then_read(&b"\x00\x00"[..])
        ;

    let io = LengthDelimited::new(io);

    assert!(support::collect_chunks(io).is_err());
}

#[test]
pub fn incomplete_head_multi() {
    let io = FixtureIo::empty()
        .then_read(&b"\x00"[..])
        .then_wait(ms(50))
        .then_read(&b"\x00"[..])
        .then_wait(ms(50))
        ;

    let io = LengthDelimited::new(io);

    assert!(support::collect_chunks(io).is_err());
}

#[test]
pub fn incomplete_payload() {
    let io = FixtureIo::empty()
        .then_read(&b"\x00\x00\x00\x09ab"[..])
        .then_wait(ms(50))
        .then_read(&b"cd"[..])
        .then_wait(ms(50))
        ;

    let io = LengthDelimited::new(io);

    assert!(support::collect_chunks(io).is_err());
}

fn expect(elems: &[&[u8]]) -> Vec<Vec<u8>> {
    elems.iter()
        .map(|e| e.iter().map(|&b| b).collect())
        .collect()
}

fn bytes(v: &[u8]) -> Vec<u8> {
    v.iter().map(|&b| b).collect()
}

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}
