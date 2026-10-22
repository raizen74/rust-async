use crate::runtime::Waker;
use mio::{Events, Interest, Poll, Registry, Token, net::TcpStream};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
};

type Wakers = Arc<Mutex<HashMap<usize, Waker>>>; // Arc because it's moved into the event loop thread, Mutex because multiple threads can access it

// OnceLock allows us to define a static variable that we can write to once so that we can initialize it when we start our Reactor
static REACTOR: OnceLock<Reactor> = OnceLock::new();

pub fn reactor() -> &'static Reactor {
    REACTOR.get().expect("Called outside an runtime context")
}

pub struct Reactor {
    wakers: Wakers,
    registry: Registry, // Interact with mio event queue
    next_id: AtomicUsize, // Reactor is multi-threaded, so we need to use an AtomicUsize to guarantee unique ids across threads
}

impl Reactor {
    // id identifies the stream
    pub fn register(&self, stream: &mut TcpStream, interest: Interest, id: usize) {
        self.registry.register(stream, Token(id), interest).unwrap();
    }

    pub fn set_waker(&self, waker: &Waker, id: usize) {
        // insert a waker for this id
        let _ = self
            .wakers
            .lock()
            // Must always store the most recent waker
            .map(|mut w| w.insert(id, waker.clone()).is_none())
            .unwrap();
    }

    pub fn deregister(&self, stream: &mut TcpStream, id: usize) {
        // remove the waker for this id
        self.wakers.lock().map(|mut w| w.remove(&id)).unwrap();
        // deregister the stream from the Poll instance, so that we don't get any more events for this stream
        self.registry.deregister(stream).unwrap();
    }

    pub fn next_id(&self) -> usize {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }
}

fn event_loop(mut poll: Poll, wakers: Wakers) {
    let mut events = Events::with_capacity(100);
    loop {
        // timeout: None means block until an event occurs
        poll.poll(&mut events, None).unwrap();
        for e in events.iter() {
            let Token(id) = e.token();
            let wakers = wakers.lock().unwrap();
            // Get the waker for that token, which may have been registered by a different thread
            if let Some(waker) = wakers.get(&id) {
                waker.wake(); // put the task_id into the ready queue and wake up the executor thread
            }
        }
    }
}

pub fn start() {
    use thread::spawn;

    let wakers = Arc::new(Mutex::new(HashMap::new()));
    let poll = Poll::new().unwrap();
    let registry = poll.registry().try_clone().unwrap();
    let next_id = AtomicUsize::new(1); // different value that the Executor's next_id for debugging
    let reactor = Reactor {
        wakers: wakers.clone(),
        registry,
        next_id,
    };

    REACTOR.set(reactor).ok().expect("Reactor already running");
    spawn(move || event_loop(poll, wakers)); // start the event loop in a separate thread
}
