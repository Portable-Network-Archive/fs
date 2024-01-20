pub(crate) mod mount;

use std::io;

pub(crate) trait Command {
    fn execute(&self) -> io::Result<()>;
}
