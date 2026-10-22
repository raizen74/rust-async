use std::time::Instant;
mod runtime;
mod future;
mod http;

use crate::runtime::Runtime;
use crate::http::Http;
use future::*;

fn main() {
    let start = Instant::now();
    let future = Coroutine1::new();
    let mut runtime = Runtime::new();
    runtime.block_on(future);

    println!("\nELAPSED TIME: {}", start.elapsed().as_secs_f32());
}

enum State0 {
    Start(usize),
    Wait1(Box<dyn Future<Output = String>>),
    Resolved,
}

struct Coroutine0 {
    state: State0,
}

impl Coroutine0 {
    fn new(i: usize) -> Self {
        Self {
            state: State0::Start(i),
        }
    }
}

impl Future for Coroutine0 {
    type Output = String;

    fn poll(&mut self) -> PollState<Self::Output> {
        loop {
            match self.state {
                State0::Start(i) => {
                    // ---- Code you actually wrote ----
                    let path = format!("/{}/HelloWorld{i}", i * 1000);

                    // ---------------------------------
                    let fut1 = Box::new(Http::get(&path));
                    self.state = State0::Wait1(fut1);
                }
                State0::Wait1(ref mut f1) => {
                    match f1.poll() {
                        PollState::Ready(txt) => {
                            println!("{txt}");
                            self.state = State0::Resolved;
                            break PollState::Ready(String::new());
                        }
                        PollState::NotReady => break PollState::NotReady,
                    }
                }
                State0::Resolved => panic!("Polled a resolved future"),
            }
        }
    }
}

enum State1 {
    Start,
    Wait1(Box<dyn Future<Output = String>>),
    Resolved,
}

struct Coroutine1 {
    state: State1,
}

impl Coroutine1 {
    fn new() -> Self {
        Self {
            state: State1::Start,
        }
    }
}

impl Future for Coroutine1 {
    type Output = String;

    fn poll(&mut self) -> PollState<Self::Output> {
        loop {
            match self.state {
                State1::Start => {
                    println!("Program starting");
                    let mut futures = vec![];
                    for i in 0..5 {
                        futures.push(Coroutine0::new(i));
                    }
                    let fut1 = Box::new(future::join_all(futures));
                    self.state = State1::Wait1(fut1); // Move to waiting state
                }
                State1::Wait1(ref mut f1) => {
                    match f1.poll() { // Poll the join_all future
                        PollState::Ready(_) => {
                            self.state = State1::Resolved;
                            break PollState::Ready(String::new());
                        }
                        PollState::NotReady => break PollState::NotReady,
                    }
                }
                State1::Resolved => panic!("Polled a resolved future"),
            }
        }
    }
}
