extern crate minidb;
extern crate docopt;
extern crate rand;
extern crate env_logger;

use minidb::config;
use minidb::client::Client;

use docopt::Docopt;

use std::process;
use std::net::SocketAddr;

const USAGE: &'static str = "
Usage: minidb-cli [--node=ID] [--causal=ID] [--node-list=FILE] <cmd> [<arg>]
       minidb-cli --help

Commands:
    get                 Get all values in the set
    insert VALUE        Insert a value into thr set
    remove VALUE        Remove a value from the set
    clear               Cleat all values from the set.

Options:

    --node=ID           ID of node to connect to.
                            Default: random

    --node-list=FILE    Path to toml file containing list of server nodes.
                            Default: etc/nodes.toml

    --causal=ID         Fetch the current state from the given node ID and perform
                        the remove based on that state.

    --help              Display this message
";


pub fn main() {
    let _ = env_logger::init();

    // Create a opt parser from the top level usage
    let args = Docopt::new(USAGE).unwrap()
        .help(true)
        .parse()
        .unwrap_or_else(|e| e.exit());

    let mut file = args.get_str("--node-list");

    if file == "" {
        file = "etc/nodes.toml";
    }

    let nodes = config::Nodes::load_from_file(file);

    // Pick a node to connect to
    let remote_addr = pick_node(nodes.clone(), args.get_str("--node"));

    let client = Client::connect(&remote_addr).unwrap();

    match args.get_str("<cmd>") {
        "get" => {
            let set = client.get().unwrap();

            if set.is_empty() {
                println!("[empty set]");
            } else {
                for elem in set.iter() {
                    println!("  - {}", elem);
                }
            }
        }
        "insert" => {
            let value = args.get_str("<arg>");

            if value.is_empty() {
                println!("value missign");
            }

            client.insert(value).unwrap();
            println!("OK");

        }
        "remove" => {
            let value = args.get_str("<arg>");

            if value.is_empty() {
                println!("value missing");
            }

            let causal = args.get_str("--causal");

            if causal != "" {
                let remote_addr = pick_node(nodes, causal);
                let get_client = Client::connect(&remote_addr).unwrap();
                let res = get_client.get().unwrap();

                client.causal_remove(value, res.version_vec()).unwrap();

            } else {
                client.remove(value).unwrap();
            }

            println!("OK");
        }
        "clear" => {
            client.clear().unwrap();
            println!("OK");
        }
        _ => {
            println!("Invalid command");
            println!("");
            println!("{:?}", USAGE);
            process::exit(1);
        }
    }
}

fn pick_node(nodes: config::Nodes, node: &str) -> SocketAddr {
    use rand::Rng;

    if node != "" {
        // Parse the node ID
        let id: u64 = node.parse().unwrap();
        nodes.get(id).local_addr().clone()
    } else {
        let mut rng = rand::thread_rng();
        let mut addrs = nodes.local_addrs();

        rng.shuffle(&mut addrs);

        addrs.remove(0)
    }
}
