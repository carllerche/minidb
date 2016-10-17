//! # Overview
//!
//! MiniDB is a toy distributed, in-memory, database demonstrating how to build
//! a full application with Tokio. It provides a set of strings, represented as
//! a CRDT. The topology is defined in a configuration file. Replication is
//! done by sending the full state to all peer nodes in the cluster.
//!
//! ## Datamodel
//!
//! MiniDB is a replicated set of strings. It is implemented as an optimized,
//! observe-remove set CRDT. The implementations for the set, version vector,
//! and other supporting types are in the [dt](dt/index.html) module.
//!
//! ## Protocol
//!
//! The protocol consists of requests and responses encoded using [protobufs]()
//! and length delimited framing. Requests and responses are multiplexed. The
//! request and response types as well as the wire protocol implementation are
//! in the [proto](proto/index.html) module.
//!
//! ## Server
//!
//! The MiniDB server manages the in-memory set, processes client requests, and
//! handles replication with peers. It is implemented as a
//! `tokio_service::Service` that responds to the requests defined in
//! [proto](proto/index.html). It uses `tokio_proto::easy` to handle the
//! multiplxing details. The implementation is in the [server](server.html)
//! module.
//!
//! ## Client
//!
//! The MiniDB client connects to any server node. The client supports `get`,
//! `insert`, `remove`, and `clear` operations. The implmentation is in the
//! [client](client.html) module.

// Future library. Used as a foundation for Tokio.
#[macro_use]
extern crate futures;

// Provides I/O reactor, TCP types, and Io / FramedIo traits
//
// Io: asynchronous io::Read + io::Write
// FramedIo: asynchronous Read + Write of values (frames)
extern crate tokio_core;

// `Service` trait: An asynchronous function from `Request` to a `Response`.
//
// The `Service` trait is a simplified interface making it easy to write
// network applications in a modular and reusable way, decoupled from the
// underlying protocol. It is one of Tokio's fundamental abstractions.
extern crate tokio_service;

// Provides glue between `FramedIo` and `Service`
extern crate tokio_proto;

// Future based timing facilities
extern crate tokio_timer;

// Utilities for manipulating bytes. Includes various buffer implementations
extern crate bytes;

// Protobuf serialization / deserialization
#[macro_use]
extern crate buffoon;

// Used for MiniDB topology configuration
extern crate toml;

// Helpers for converting [u8] <-> integers
extern crate byteorder;

// Random number generation
extern crate rand;

// Logging
#[macro_use]
extern crate log;

pub mod config;
pub mod dt;
pub mod proto;
pub mod client;
pub mod server;

mod peer;
