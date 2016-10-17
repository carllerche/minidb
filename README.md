# MiniDB

MiniDB is a very simple (toy) distributed in-memory database. It is
built in [Rust](http://rust-lang.org) using
[Tokio](https://github.com/tokio-rs/tokio) for the networking layer.

## Building MiniDB

Ensure that you are using the latest stable release of
[Rust](http://rust-lang.org). Clone this repo locally, then run:

```
cargo build
```

That's it.

## Configuring

The MiniDB topology is configured by modifying the file at
`etc/nodes.toml`. All MiniDB nodes are assumed to run on localhost with
different ports

The configuration file looks like:

```toml
[[node]]
id = 1
port = 5981

[[node]]
id = 2
port = 5982
```

## Starting the cluster

A MiniDB node is started with the `minidb` command and takes the node ID
as an argument. The port to bind to is looked up in the topology
configuration file.

For example:

```
minidb --node=1
```

For more details, run:

```
minidb -h
```

## Interacting with the cluster

The `minidb-cli` tool is a CLI based client to MiniDB which allows
issuing the basic commands.



## License

`MiniDB` is primarily distributed under the terms of both the MIT
license and the Apache License (Version 2.0), with portions covered by
various BSD-like licenses.

See LICENSE-APACHE, and LICENSE-MIT for details.
