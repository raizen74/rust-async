use crate::future::{Future, PollState};
use mio::{Events, Poll, Registry};
use std::sync::OnceLock;

static REGISTRY: OnceLock<Registry> = OnceLock::new();

// make the registry available at http::HttpGetFuture::poll to register interest in the first poll
pub fn registry() -> &'static Registry {
    REGISTRY.get().expect("Called outside a runtime context")
}

pub struct Runtime {
    poll: Poll,
}

impl Runtime {
    pub fn new() -> Self {
        let poll = Poll::new().unwrap();
        let registry = poll.registry().try_clone().unwrap();
        /*
        initialize REGISTRY when the runtime starts,
        thereby preventing anyone (including ourselves) from calling Http::get without having a
        Runtime instance running
        */ 
        REGISTRY.set(registry).unwrap();
        Self { poll }
    }
    pub fn block_on<F>(&mut self, future: F)
    where
        F: Future<Output = String>,
    {
        let mut future = future;
        // loop until the top level future returns READY
        loop {
            match future.poll() {
                PollState::NotReady => {
                    println!("Schedule other tasks\n");
                    let mut events = Events::with_capacity(100);
                    // This call blocks until the self.poll.registry() is notified
                    // The OS wake us up when an event has happened
                    // Both the runtime and the reactor work with the registry, introducing coupling
                    // This means that runtime is blocked by 1 reactor (Poll)
                    self.poll.poll(&mut events, None).unwrap();
                }
                PollState::Ready(_) => break,
            }
        }
    }
}
