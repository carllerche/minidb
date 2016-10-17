//! Request / response types, `FramedIo` implementation

mod length_delimited;
mod junkify;
mod transport;

pub use self::length_delimited::LengthDelimited;
pub use self::transport::Transport;

use dt::{Set, VersionVec};
use buffoon::*;
use std::io;

/// MiniDB request message
///
/// This is issued both by clients to a server, and servers to their peers.
/// However, it is expected that the client only uses `Get`, `Insert`,
/// `Remove`, and `Clear` and that peers use the `Join` variant.
///
/// Not the cleanest... but it means that less code is needed.
#[derive(Debug)]
pub enum Request {
    Get(Get),
    Insert(Insert),
    Remove(Remove),
    Clear(Clear),
    Join(Set<String>),
}

/// MiniDB response message, this is the pair to `Request`
#[derive(Debug)]
pub enum Response {
    Success(Success),
    Value(Set<String>),
}

/// Get the set of values
#[derive(Debug)]
pub struct Get;

/// Insert a new value into the set
#[derive(Debug)]
pub struct Insert {
    // value to insert
    value: String,
}

/// Remove a value from the set
#[derive(Debug)]
pub struct Remove {
    // value to insert
    value: String,
    // If `causality` is set, then only remove the value from the state
    // represented by the provided version vector
    causality: Option<VersionVec>,
}

/// Clear th eset
#[derive(Debug)]
pub struct Clear;

/// Successful operation
#[derive(Debug)]
pub struct Success;

/*
 *
 * ===== impl Request =====
 *
 */

impl Serialize for Request {
    fn serialize<O: OutputStream>(&self, out: &mut O) -> io::Result<()> {
        use self::Request::*;

        match *self {
            Get(ref v) => out.write(1, v),
            Insert(ref v) => out.write(2, v),
            Remove(ref v) => out.write(3, v),
            Clear(ref v) => out.write(4, v),
            Join(ref v) => out.write(5, v),
        }
    }
}

impl Deserialize for Request {
    fn deserialize<R: io::Read>(i: &mut InputStream<R>) -> io::Result<Request> {
        use self::Request::*;

        let mut r = None;

        while let Some(f) = try!(i.read_field()) {
            match f.tag() {
                1 => r = Some(Get(try!(f.read()))),
                2 => r = Some(Insert(try!(f.read()))),
                3 => r = Some(Remove(try!(f.read()))),
                4 => r = Some(Clear(try!(f.read()))),
                5 => r = Some(Join(try!(f.read()))),
                _ => try!(f.skip()),
            }
        }

        Ok(required!(r))
    }
}

/*
 *
 * ===== impl Response =====
 *
 */

impl Serialize for Response {
    fn serialize<O: OutputStream>(&self, out: &mut O) -> io::Result<()> {
        use self::Response::*;

        match *self {
            Success(ref v) => out.write(1, v),
            Value(ref v) => out.write(2, v),
        }
    }
}

impl Deserialize for Response {
    fn deserialize<R: io::Read>(i: &mut InputStream<R>) -> io::Result<Response> {
        use self::Response::*;

        let mut r = None;

        while let Some(f) = try!(i.read_field()) {
            match f.tag() {
                1 => r = Some(Success(try!(f.read()))),
                2 => r = Some(Value(try!(f.read()))),
                _ => try!(f.skip()),
            }
        }

        Ok(required!(r))
    }
}

/*
 *
 * ===== impl Get =====
 *
 */

impl Serialize for Get {
    fn serialize<O: OutputStream>(&self, _: &mut O) -> io::Result<()> {
        Ok(())
    }
}

impl Deserialize for Get {
    fn deserialize<R: io::Read>(i: &mut InputStream<R>) -> io::Result<Get> {
        while let Some(f) = try!(i.read_field()) {
            try!(f.skip());
        }

        Ok(Get)
    }
}

/*
 *
 * ===== impl Insert =====
 *
 */

impl Insert {
    pub fn value(&self) -> &str {
        &self.value
    }
}

impl Serialize for Insert {
    fn serialize<O: OutputStream>(&self, out: &mut O) -> io::Result<()> {
        out.write(1, &self.value)
    }
}

impl Deserialize for Insert {
    fn deserialize<R: io::Read>(i: &mut InputStream<R>) -> io::Result<Insert> {
        let mut value = None;

        while let Some(f) = try!(i.read_field()) {
            match f.tag() {
                1 => value = Some(try!(f.read())),
                _ => try!(f.skip()),
            }
        }

        Ok(Insert { value: required!(value) })
    }
}

impl<T: Into<String>> From<T> for Insert {
    fn from(src: T) -> Insert {
        Insert { value: src.into() }
    }
}

/*
 *
 * ===== impl Remove =====
 *
 */

impl Remove {
    pub fn new(value: String, causality: Option<VersionVec>) -> Remove {
        Remove {
            value: value,
            causality: causality,
        }
    }

    pub fn value(&self) -> &str {
        &self.value
    }

    pub fn causality(&self) -> Option<&VersionVec> {
        self.causality.as_ref()
    }
}

impl Serialize for Remove {
    fn serialize<O: OutputStream>(&self, out: &mut O) -> io::Result<()> {
        try!(out.write(1, &self.value));
        try!(out.write(2, &self.causality));

        Ok(())
    }
}

impl Deserialize for Remove {
    fn deserialize<R: io::Read>(i: &mut InputStream<R>) -> io::Result<Remove> {
        let mut value = None;
        let mut causality = None;

        while let Some(f) = try!(i.read_field()) {
            match f.tag() {
                1 => value = Some(try!(f.read())),
                2 => causality = Some(try!(f.read())),
                _ => try!(f.skip()),
            }
        }

        Ok(Remove {
            value: required!(value),
            causality: causality,
        })
    }
}

impl<T: Into<String>> From<T> for Remove {
    fn from(src: T) -> Remove {
        Remove { value: src.into(), causality: None }
    }
}

/*
 *
 * ===== impl Clear =====
 *
 */

impl Serialize for Clear {
    fn serialize<O: OutputStream>(&self, _: &mut O) -> io::Result<()> {
        Ok(())
    }
}

impl Deserialize for Clear {
    fn deserialize<R: io::Read>(i: &mut InputStream<R>) -> io::Result<Clear> {
        while let Some(f) = try!(i.read_field()) {
            try!(f.skip());
        }

        Ok(Clear)
    }
}

/*
 *
 * ===== impl Success =====
 *
 */

impl Serialize for Success {
    fn serialize<O: OutputStream>(&self, _: &mut O) -> io::Result<()> {
        Ok(())
    }
}

impl Deserialize for Success {
    fn deserialize<R: io::Read>(i: &mut InputStream<R>) -> io::Result<Success> {
        while let Some(f) = try!(i.read_field()) {
            try!(f.skip());
        }

        Ok(Success)
    }
}
