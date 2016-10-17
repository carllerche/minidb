#![allow(warnings)]

mod fuzz;
mod io;

pub use self::fuzz::Fuzz;
pub use self::io::collect_chunks;
