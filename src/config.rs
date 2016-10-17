//! Topology configuration
//!
//! The topology configuration is represented as a TOML configuration file. It
//! contains the list of all nodes in the MiniDB cluster as well as describes
//! how to "junkify" the connections.
//!
//! Junkifying a connection takes a TCP connection and turns it into junk aka
//! simulates poor network connectivity.

use toml;

use std::{fmt,io};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct Nodes {
    inner: Arc<Mutex<Inner>>,
}

#[derive(Clone)]
pub struct Node {
    inner: Arc<Mutex<Inner>>,
    node_id: u64,
    local_addr: SocketAddr,
}

#[derive(Clone)]
pub struct Route {
    inner: Arc<Mutex<Inner>>,
    src: u64,
    dst: u64,
    remote_addr: SocketAddr,
}

/// How an `Io` processes written data
#[derive(Debug, Copy, Clone)]
pub enum Action {
    Allow,
    Deny,
    Delay,
}

#[derive(Debug)]
struct Inner {
    nodes: Vec<NodeConfig>,
    default_action: Action,
}

#[derive(Debug)]
struct NodeConfig {
    node_id: u64,
    addr: SocketAddr,
    routes: Vec<RouteConfig>,
}

#[derive(Debug)]
struct RouteConfig {
    destination: u64,
    action: Action,
}

impl Node {
    pub fn local_addr(&self) -> &SocketAddr {
        &self.local_addr
    }

    pub fn routes(&self) -> Vec<Route> {
        let inner = self.inner.lock().unwrap();

        inner.nodes.iter()
            .filter(|n| n.node_id != self.node_id)
            .map(|n| {
                Route {
                    inner: self.inner.clone(),
                    src: self.node_id,
                    dst: n.node_id,
                    remote_addr: n.addr.clone(),
                }
            })
            .collect()
    }
}

impl Route {
    pub fn remote_addr(&self) -> &SocketAddr {
        &self.remote_addr
    }

    pub fn destination(&self) -> u64 {
        self.dst
    }

    pub fn action(&self) -> Action {
        let inner = self.inner.lock().unwrap();

        inner.nodes.iter()
            .find(|n| n.node_id == self.src)
            .and_then(|n| {
                n.routes.iter()
                    .find(|r| r.destination == self.dst)
                    .map(|r| r.action)
            })
            .unwrap_or(inner.default_action)
    }
}

impl Nodes {
    pub fn load_from_file(file: &str) -> Nodes {
        let inner = Inner {
            nodes: vec![],
            default_action: Action::Allow,
        };

        let nodes = Nodes {
            inner: Arc::new(Mutex::new(inner)),
        };

        nodes.reload(file).unwrap();

        // Reload config from thread every few seconds
        {
            let nodes = nodes.clone();
            let file: String = file.into();

            ::std::thread::spawn(move || {
                use std::thread;
                use std::time::Duration;

                loop {
                    thread::sleep(Duration::from_secs(3));

                    if let Err(e) = nodes.reload(&file) {
                        warn!("failed to reload minidb config; {:?}", e);
                    } else {
                        debug!("reloaded routes");
                    }
                }
            });
        }

        nodes
    }

    pub fn get(&self, id: u64) -> Node {
        let inner = self.inner.lock().unwrap();

        let node = inner.nodes.iter().find(|n| n.node_id == id)
            .expect("no node with requested ID");

        let addr = node.addr.clone();

        Node {
            inner: self.inner.clone(),
            node_id: id,
            local_addr: addr,
        }
    }

    pub fn local_addrs(&self) -> Vec<SocketAddr> {
        let inner = self.inner.lock().unwrap();

        inner.nodes.iter()
            .map(|n| n.addr.clone())
            .collect()
    }

    fn reload(&self, file: &str) -> io::Result<()> {
        let values = try!(self.parse_file(file));

        let default_action = match values.get("default_action") {
            Some(value) => try!(parse_action(value)),
            None => Action::Allow,
        };

        let node_config = try!(values.get("node").and_then(toml::Value::as_slice)
                               .ok_or(convert("expected [[node]]")));

        let mut nodes = vec![];

        // Load data
        for cfg in node_config {
            let cfg = try!(cfg.as_table().ok_or(convert("expected table under [[node]]")));

            let port = try!(cfg.get("port").and_then(toml::Value::as_integer)
                            .ok_or(convert("expected `port` integer under [[node]]")));

            if port < 0 || port > 1_000_000 {
                return Err(convert("invalid port"));
            }

            let id = try!(cfg.get("id").and_then(toml::Value::as_integer)
                            .ok_or(convert("expected `id` integer under [[node]]")));

            let host = format!("127.0.0.1:{}", port);
            let addr: SocketAddr = host.parse().unwrap();

            let mut node = NodeConfig {
                node_id: id as u64,
                addr: addr,
                routes: vec![],
            };

            let routes = cfg.get("routes").and_then(toml::Value::as_slice).unwrap_or(&[]);

            for cfg in routes {
                let cfg = try!(cfg.as_table().ok_or(convert("expected node.routes to be a table")));

                let id = try!(cfg.get("destination").and_then(toml::Value::as_integer)
                                .ok_or(convert("expected `destination` integer under `routes`")));

                let action = try!(cfg.get("action").ok_or(convert("nope")).and_then(parse_action));

                node.routes.push(RouteConfig {
                    destination: id as u64,
                    action: action,
                });
            }

            nodes.push(node);
        }

        let mut inner = self.inner.lock().unwrap();

        inner.default_action = default_action;
        inner.nodes = nodes;

        Ok(())
    }

    /// Parse the config file into a toml value
    fn parse_file(&self, file: &str) -> io::Result<toml::Table> {
        use std::fs::{File};
        use std::io::Read;

        let mut file = try!(File::open(file));
        let mut dst = vec![];

        try!(file.read_to_end(&mut dst));

        let data = try!(String::from_utf8(dst).map_err(convert));

        // Parse the config file contents into a toml value
        match toml::Parser::new(&data).parse() {
            Some(value) => Ok(value),
            None => Err(convert("empty toml file")),
        }
    }
}

fn parse_action(val: &toml::Value) -> io::Result<Action> {
    let val = val.as_str().ok_or(convert("expected action"));

    match try!(val) {
        "allow" => Ok(Action::Allow),
        "deny" => Ok(Action::Deny),
        "delay" => Ok(Action::Delay),
        _ => Err(convert("invalid action")),
    }
}

fn convert<E: Into<Box<::std::error::Error + Send + Sync>>>(e: E) -> io::Error {
    io::Error::new(io::ErrorKind::Other, e)
}

impl fmt::Debug for Nodes {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        self.inner.lock().unwrap().fmt(fmt)
    }
}
