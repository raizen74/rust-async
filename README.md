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