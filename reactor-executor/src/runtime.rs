mod reactor;
mod executor;
pub use executor::{Executor, Waker, spawn}; // re-export Executor, Waker, and spawn from the executor module
pub use reactor::reactor;

pub fn init() -> Executor {
    reactor::start();
    Executor::new()
}
