/// Adds a file descriptor to an epoll instance with `epoll_ctl`.
pub const EPOLL_CTL_ADD: i32 = 1;
/// Interest flag for readiness to read without blocking.
///
/// Combine this with other interest flags using bitwise OR.
pub const EPOLLIN: i32 = 0x1;
/// Interest modifier that requests edge-triggered notifications.
///
/// Combine this with readiness flags such as [`EPOLLIN`] using bitwise OR.
pub const EPOLLET: i32 = 1 << 31;

#[link(name = "c")]
unsafe extern "C" {
    /// Creates an epoll instance.
    ///
    /// `size` must be positive; it is ignored by modern Linux kernels.
    /// Returns the new epoll file descriptor, or `-1` on error with `errno` set.
    pub fn epoll_create(size: i32) -> i32;

    /// Closes a file descriptor.
    ///
    /// `fd` is the descriptor to close. Returns `0` on success, or `-1` on error
    /// with `errno` set.
    pub fn close(fd: i32) -> i32;

    /// Adds, modifies, or removes a file descriptor from an epoll instance.
    ///
    /// `epfd` is the epoll descriptor, `op` is the operation to perform, `fd`
    /// is the descriptor to operate on, and `event` points to its event settings
    /// (unused when removing a descriptor). Returns `0` on success, or `-1` on
    /// error with `errno` set.
    pub fn epoll_ctl(epfd: i32, op: i32, fd: i32, event: *mut Event) -> i32;

    /// Waits for events on an epoll instance.
    ///
    /// `epfd` is the epoll descriptor, `events` points to an output array,
    /// `maxevents` is its capacity, and `timeout` is the wait limit in
    /// milliseconds (`-1` waits indefinitely, `0` does not wait). Returns the
    /// number of ready events (possibly `0` on timeout), or `-1` on error with
    /// `errno` set.
    pub fn epoll_wait(epfd: i32, events: *mut Event, maxevents: i32, timeout: i32) -> i32;
}

#[derive(Debug)]
#[repr(C)]
#[cfg_attr(target_arch = "x86_64", repr(packed))]
pub struct Event {
    // Bitmask of events that occurred: 1 for readable, 2 for writable, etc.
    pub(crate) events: u32,
    // Token to identify event
    pub(crate) epoll_data: usize,
}

impl Event {
    pub fn token(&self) -> usize {
        self.epoll_data
    }
}
