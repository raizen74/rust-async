- [epoll](#epoll)
  - [How It Works](#how-it-works)
  - [Linux Syscalls](#linux-syscalls)
  - [Run the Example](#run-the-example)
  - [Platform](#platform)
- [coroutine crate](#coroutine-crate)
  - [Big Picture](#big-picture)
  - [Core Types and Responsibilities](#core-types-and-responsibilities)
    - [`future::Future` and `PollState`](#futurefuture-and-pollstate)
    - [`Coroutine0` (single request task)](#coroutine0-single-request-task)
    - [`HttpGetFuture` (non-blocking I/O future)](#httpgetfuture-non-blocking-io-future)
    - [`join_all` (fan-out + aggregate completion)](#join_all-fan-out--aggregate-completion)
    - [`Coroutine1` (top-level orchestrator)](#coroutine1-top-level-orchestrator)
  - [How the Polling Workflow Achieves Concurrency](#how-the-polling-workflow-achieves-concurrency)
  - [Mental Model: "Advance One Step Per Poll"](#mental-model-advance-one-step-per-poll)
  - [Running](#running)
  - [Notes and Limits](#notes-and-limits)


# epoll

This workspace crate is a small Linux I/O polling example built directly on
the `epoll` system calls. It demonstrates waiting for readiness notifications
from multiple nonblocking TCP streams without assigning a thread to each
connection. It is an educational example, not a general-purpose async runtime.

## How It Works

- `ffi` declares the C interfaces for creating, controlling, waiting on, and
  closing an epoll instance. It also defines the event structure and the
  `EPOLLIN` and `EPOLLET` bitflags.
- `Poll` owns the epoll instance and waits for events. Its `poll` method accepts
  a timeout in milliseconds; `None` waits indefinitely.
- `Registry` adds a `TcpStream` to the poller. Each registration has a token,
  which is returned with notified events so the caller can identify the stream.
- The example opens five nonblocking connections to the workspace's
  `delayserver`, registers read readiness with edge-triggered notifications,
  and reads each response until the stream reaches EOF.

The registration API currently supports adding TCP streams with
`EPOLL_CTL_ADD`; it does not expose modification or removal operations.

## Linux Syscalls

The FFI layer declares these Linux C functions using Rust `i32` for the C `int`
arguments and return values:

| Syscall | Purpose | Parameters | Return value |
| --- | --- | --- | --- |
| `epoll_create(size: i32) -> i32` | Creates an epoll instance for monitoring file descriptors. | `size` must be positive. Modern Linux kernels ignore it. | The new epoll file descriptor, or `-1` on error with `errno` set. |
| `close(fd: i32) -> i32` | Closes a file descriptor and releases the associated resource. | `fd` is the file descriptor to close. | `0` on success, or `-1` on error with `errno` set. |
| `epoll_ctl(epfd: i32, op: i32, fd: i32, event: *mut Event) -> i32` | Adds, modifies, or removes a file descriptor from an epoll instance. | `epfd` is the epoll file descriptor; `op` selects the operation (`EPOLL_CTL_ADD` adds a descriptor); `fd` is the descriptor affected; `event` contains Event settings: interests in `EPOLLIN` and `EPOLLET` and `token` identifying the `fd` and is unused when removing a descriptor. | `0` on success, or `-1` on error with `errno` set. |
| `epoll_wait(epfd: i32, events: *mut Event, maxevents: i32, timeout: i32) -> i32` | Waits for readiness notifications from descriptors registered with an epoll instance. | `epfd` is the epoll file descriptor; `events` points to the output array; `maxevents` is its capacity; `timeout` is in milliseconds (`-1` waits indefinitely, `0` returns without waiting). | The number of ready events (possibly `0` on timeout), or `-1` on error with `errno` set. |

## Run the Example

The example expects the delay server to be listening on `localhost:8080`.
Start it in one terminal from the workspace root:

```sh
cargo run -p delayserver
```

Then, in another terminal from the workspace root, run the epoll example:

```sh
cargo run -p epoll
```

The server logs each request and waits for the delay encoded in its path before
responding. The client prints epoll notifications and the response data, then
exits after all five streams have reached EOF.

## Platform

This crate calls Linux `epoll` APIs directly and is intended for Linux targets.

# coroutine crate

This crate demonstrates a minimal async runtime pattern built from scratch:
- A custom `Future` trait (`poll` + `PollState`)
- Explicit state-machine coroutines
- Non-blocking HTTP I/O
- Cooperative concurrency via repeated polling

## Big Picture

`main` repeatedly polls a top-level future (`Coroutine1`) until it returns `Ready`.

Each call to `poll()` does a *small amount of work* and then returns:
- `PollState::NotReady` when more progress is needed later
- `PollState::Ready(T)` when the task is complete

That repeated polling loop is the scheduler in this example.

## Core Types and Responsibilities

### `future::Future` and `PollState`

In `src/future.rs`:
- `Future` defines `fn poll(&mut self) -> PollState<Self::Output>`
- `PollState::Ready(T)` means done
- `PollState::NotReady` means try again later

This contract is what allows many in-flight operations to make incremental progress.

### `Coroutine0` (single request task)

In `src/main.rs`, `Coroutine0` models one request with explicit states:
- `State0::Start(i)`
- `State0::Wait1(Box<dyn Future<Output = String>>)`
- `State0::Resolved`

Polling behavior:
1. `Start(i)` builds a path and creates `Http::get(path)` future.
2. State moves to `Wait1(...)`.
3. In `Wait1`, it polls the HTTP future.
4. If HTTP is `NotReady`, `Coroutine0` returns `NotReady`.
5. If HTTP is `Ready(response)`, it prints response, transitions to `Resolved`, and returns `Ready`.

So one `Coroutine0` poll either initializes work or advances/readies that one request.

### `HttpGetFuture` (non-blocking I/O future)

In `src/http.rs`, `Http::get` returns `HttpGetFuture`.

Its internal state is implicit in fields:
- `stream: Option<mio::net::TcpStream>`
- `buffer: Vec<u8>`
- `path: String`

Polling behavior:
1. First poll: connect, set non-blocking, write request, return `NotReady`.
2. Later polls: attempt `read` loop.
3. If read returns bytes, append to `buffer` and keep reading in same poll.
4. If read returns `WouldBlock`, return `NotReady` (socket not ready yet).
5. If read returns `0` (EOF), convert buffer to string and return `Ready(response)`.

This is the key non-blocking behavior: `WouldBlock` means "not done yet", not failure.

### `join_all` (fan-out + aggregate completion)

In `src/future.rs`, `join_all` stores `(finished, future)` pairs plus `finished_count`.

On each `poll()` call:
- Iterate all child futures.
- Skip already-finished ones.
- Poll unfinished ones once each.
- Mark each newly-ready future as finished.

Result:
- `Ready` only when all children are done.
- Otherwise `NotReady`.

This is where concurrency emerges: multiple request futures are advanced in a round-robin polling pass.

### `Coroutine1` (top-level orchestrator)

In `src/main.rs`, `Coroutine1` states:
- `State1::Start`
- `State1::Wait1(Box<dyn Future<Output = String>>)`
- `State1::Resolved`

Polling behavior:
1. `Start`: builds 5 `Coroutine0` request futures.
2. Wraps them in `join_all`.
3. Transitions to `Wait1`.
4. In `Wait1`, polls `join_all`.
5. Returns `Ready` once all 5 requests complete.

## How the Polling Workflow Achieves Concurrency

No thread pool is required here. Concurrency comes from repeatedly advancing many state machines:

1. `main` calls `Coroutine1.poll()` in a loop.
2. `Coroutine1` polls `join_all`.
3. `join_all` polls each `Coroutine0`.
4. Each `Coroutine0` polls its `HttpGetFuture`.
5. Any socket that is not ready returns `WouldBlock -> NotReady` quickly.
6. The loop moves on to other futures instead of blocking on one request.

So each iteration of the `for` loop inside `join_all.poll()` advances every unfinished future by one step. Over many outer loop iterations, all requests complete.

## Mental Model: "Advance One Step Per Poll"

Think of every future as a tiny state machine:
- It remembers where it left off (`state` fields and internal buffers).
- `poll()` advances it as far as possible *right now*.
- If it cannot continue without waiting on I/O, it yields `NotReady`.

That is exactly what "advance the state machine at each iteration" means in this crate.

## Running

The example expects the delay server to be listening on `localhost:8080`.
Start it in one terminal from the workspace root:

```sh
cargo run -p delayserver
```
From workspace root:

```bash
cargo run -p coroutine
```

Make sure the local HTTP server expected at `127.0.0.1:8080` is running.

## Notes and Limits

- This is educational runtime code, not production async.
- It uses busy polling in `main` (no event loop wakeups).
- `join_all` output is currently a placeholder `String`.
- Errors are mostly handled with `panic!`/`unwrap()` for simplicity.
