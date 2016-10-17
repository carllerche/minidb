#![allow(warnings)]

extern crate minidb;
extern crate docopt;
extern crate env_logger;

use minidb::{config, server};
use minidb::dt::ActorId;

use docopt::Docopt;

use std::collections::HashMap;
use std::net::SocketAddr;

const USAGE: &'static str = "
Usage: minidb --node=ID [--node-list=FILE]
       minidb --help

Options:
    --node=ID           Port to run the server on

    --node-list=FILE    Path to TOML file containing peers.
                            Default: etc/peers.toml

    -h, --help          Display this message
";

pub fn main() {
    let _ = ::env_logger::init();

    // Create a opt parser from the top level usage
    let args = Docopt::new(USAGE).unwrap()
        .parse()
        .unwrap_or_else(|e| e.exit());

    let mut file = args.get_str("--node-list");

    if file == "" {
        file = "etc/nodes.toml";
    }

    // Load the toplogy configuration
    let nodes = config::Nodes::load_from_file(file);

    let node_id = args.get_str("--node");

    if node_id == "" {
        panic!("node id not specified");
    }

    let node_id: u64 = node_id.parse().unwrap();

    let config = nodes.get(node_id);
    println!("Starting server; address={}", config.local_addr());

    // Start the server
    server::run(config).unwrap();
}
