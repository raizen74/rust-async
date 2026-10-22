mod future;
mod http;
mod runtime;

use future::{Future, PollState};
use http::Http;
use runtime::Waker;
use std::time::Instant;

// This function demonstrates 2 executors in separate threads awaken by the same Reactor
fn parallel_executors() {
    use std::thread::Builder;
    let mut executor = runtime::init();
    let mut handles = vec![];
    // Spawn a few threads to run executors in parallel
    for i in 1..2 {
        let name = format!("thread-{i}");
        let h = Builder::new()
            .name(name)
            .spawn(move || {
                let mut executor = runtime::Executor::new();
                executor.block_on(Coroutine1::new());
            })
            .unwrap();
        handles.push(h);
    }
    executor.block_on(Coroutine1::new());
    handles.into_iter().for_each(|h| h.join().unwrap());
}

fn main() {
    let start = Instant::now();
    // Initialize the reactor and get the executor for the main thread
    let mut executor = runtime::init();
    executor.block_on(Coroutine1::new());
    // uncomment the following line to run the parallel executors test
    // parallel_executors();

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

    fn poll(&mut self, waker: &Waker) -> PollState<Self::Output> {
        loop {
            match self.state {
                State0::Start(i) => {
                    let path = format!("/{}/HelloWorld{i}", i * 1000);
                    // Build a leaf future a move it to the next state
                    let fut1 = Box::new(Http::get(&path));
                    println!("Future {i} transitioning to Wait1 state");
                    self.state = State0::Wait1(fut1);
                }

                State0::Wait1(ref mut f1) => {
                    // The first poll of HttpGetFuture registers interest
                    match f1.poll(waker) {
                        PollState::Ready(txt) => {
                            println!("{txt}");
                            self.state = State0::Resolved;
                            break PollState::Ready(String::new());
                        }
                        PollState::NotReady => {
                            break PollState::NotReady
                        },
                    }
                }

                State0::Resolved => panic!("Polled a resolved future"),
            }
        }
    }
}

enum State1 {
    Start,
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

    fn poll(&mut self, waker: &Waker) -> PollState<Self::Output> {
        loop {
            match self.state {
                State1::Start => {
                    // ---- Code you actually wrote ----
                    println!("Program starting Coroutine1, spawning 5 Coroutine0 futures");

                    for i in 0..5 {
                        runtime::spawn(Coroutine0::new(i)); // Add tasks to ExecutorCore
                    }

                    // ---------------------------------
                    self.state = State1::Resolved;
                    println!("Coroutine1 finished.");
                    break PollState::Ready(String::new());
                }

                State1::Resolved => panic!("Polled a resolved future"),
            }
        }
    }
}
