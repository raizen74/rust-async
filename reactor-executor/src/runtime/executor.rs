use crate::future::{Future, PollState};
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread::{self, Thread};

#[derive(Clone)]
pub struct Waker {
    thread: Thread,                      // handle to the Thread object
    id: usize,                           // identifies which task is associated with that Waker
    ready_queue: Arc<Mutex<Vec<usize>>>, // Shared across threads: usize represents the id associated with the task
}

impl Waker {
    pub fn wake(&self) {
        self.ready_queue
            .lock()
            .map(|mut q| q.push(self.id)) // puts the task id into the ready queue
            .unwrap();
        self.thread.unpark(); // wakes up the executor thread, finds the task associated with this Waker in the ready queue and calls poll() on it
    }
}

type Task = Box<dyn Future<Output = String>>;

// thread local static variable, unique to the thread it's first called from
// It's impossible to access another thread's CURRENT_EXEC
thread_local! {
    static CURRENT_EXEC: ExecutorCore = ExecutorCore::default();
}

// ExecutorCore holds execution state
#[derive(Default)]
struct ExecutorCore {
    tasks: RefCell<HashMap<usize, Task>>, // key is the task id, value is the top level Future
    /*
        An Arc is given to each Waker that this executor creates, the Waker pushes the task_id
        into the ready_queue when it's ready Waker::wake(), the Waker is in another thread
    */
    ready_queue: Arc<Mutex<Vec<usize>>>,
    next_id: Cell<usize>, // Guarantees unique future ids. Cell gives interior mutability in the same thread ExecutorCore lives
}

pub fn spawn<F>(future: F)
where
    F: Future<Output = String> + 'static, // 'static means the future can live for the entire duration of the program, cannot be constrained in any way
{
    CURRENT_EXEC.with(|e| {
        let id = e.next_id.get(); // get the next unique id
        e.tasks.borrow_mut().insert(id, Box::new(future));
        e.ready_queue.lock().map(|mut q| q.push(id)).unwrap();
        e.next_id.set(id + 1); // get the next unique id
    });
}

pub struct Executor;

impl Executor {
    pub fn new() -> Self {
        Self {} // no initialization, initialization is done lazily in thread_local! macro
    }

    // takes a lock on read_queue and pops off an ID that’s ready from the back of Vec (LIFO)
    fn pop_ready(&self) -> Option<usize> {
        CURRENT_EXEC.with(|q| q.ready_queue.lock().map(|mut q| q.pop()).unwrap())
    }

    // removes and returns the future from the tasks collection
    fn get_future(&self, id: usize) -> Option<Task> {
        CURRENT_EXEC.with(|q| q.tasks.borrow_mut().remove(&id))
    }

    // creates a Waker for the task with the given id
    fn get_waker(&self, id: usize) -> Waker {
        Waker {
            id,
            thread: thread::current(),
            ready_queue: CURRENT_EXEC.with(|q| q.ready_queue.clone()),
        }
    }

    // inserts the future back into the tasks collection
    fn insert_task(&self, id: usize, task: Task) {
        CURRENT_EXEC.with(|q| q.tasks.borrow_mut().insert(id, task));
    }

    // returns the number of tasks in the tasks collection
    fn task_count(&self) -> usize {
        CURRENT_EXEC.with(|q| q.tasks.borrow().len())
    }
/*
block_on will be the entry point to our Executor. Often, you will pass in one top-level future
first, and when the top-level future progresses, it will spawn new top-level futures onto our executor.
Each new future can, of course, spawn new futures onto the Executor too, and that’s how an
asynchronous program basically works
*/
    pub fn block_on<F>(&mut self, future: F)
    where
        F: Future<Output = String> + 'static,
    {
        spawn(future); // modifies ExecutorCore adding the future to the ready_queue to be polled for the first time
        // After spawning the top level Future, loop until all tasks are finished, and the ready_queue is empty
        loop {
            // pop the end of the ready_queue, which is a LIFO queue, and get the id of the task that’s ready to be polled
            while let Some(id) = self.pop_ready() {
                // remove the future from the hashmap
                let mut future = match self.get_future(id) {
                    Some(f) => f,
                    // guard against false wakeups, mio doesn’t guarantee that false wakeups won’t happen
                    None => continue,
                };
                // Waker holds the Executor thread handle so it can unpark it
                let waker = self.get_waker(id);
                // The waker will travel down to the leaf future, which stores it in the Reactor
                match future.poll(&waker) {
                    // insert the future back into the hashmap if it’s not ready yet, but not into the ready_queue
                    // this is the reactor responsibility: it will call Waker::wake() which pushes the task into the ready_queue
                    PollState::NotReady => self.insert_task(id, future),
                    PollState::Ready(_) => continue, // drop the future if it’s ready, it’s done and we don’t need to poll it again
                }
            }

            let task_count = self.task_count();
            let name = thread::current().name().unwrap_or_default().to_string();

            if task_count > 0 {
                println!("{name}: {task_count} pending tasks. Sleep until notified.");
                thread::park(); // yield control to the OS scheduler which puts the thread to sleep
            } else {
                println!("{name}: All tasks are finished");
                break;
            }
        }
    }
}
