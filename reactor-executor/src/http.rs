use crate::{future::PollState, runtime::{self, reactor, Waker}, Future};
use mio::Interest;
use std::io::{ErrorKind, Read, Write};

fn get_req(path: &str) -> String {
    format!(
        "GET {path} HTTP/1.1\r\n\
            Host: localhost\r\n\
            Connection: close\r\n\
            \r\n"
    )
}

pub struct Http;

impl Http {
    pub fn get(path: &str) -> impl Future<Output = String> + use<> {
        HttpGetFuture::new(path)
    }
}

// Leaf future that we want to register in the Reactor
struct HttpGetFuture {
    stream: Option<mio::net::TcpStream>,
    buffer: Vec<u8>,
    path: String,
    id: usize, // identifies the Future, in this case the stream
}

impl HttpGetFuture {
    fn new(path: &str) -> Self {
        let id = reactor().next_id();
        Self {
            stream: None,
            buffer: vec![],
            path: path.to_string(),
            id,
        }
    }
    // Non blocking write of the HTTP request
    fn write_request(&mut self) {
        let stream = std::net::TcpStream::connect("127.0.0.1:8080").unwrap();
        stream.set_nonblocking(true).unwrap();
        let mut stream = mio::net::TcpStream::from_std(stream);
        stream.write_all(get_req(&self.path).as_bytes()).unwrap();
        self.stream = Some(stream);
    }
}

impl Future for HttpGetFuture {
    type Output = String;

    fn poll(&mut self, waker: &Waker) -> PollState<Self::Output> {
        // If this is first time polled, start the operation
        if self.stream.is_none() {
            println!("FIRST POLL - START OPERATION");
            self.write_request();
            // CHANGED
            let stream = self.stream.as_mut().unwrap();
            // Register interest in the Reactor Poll instance for this stream, so that we can be notified when it’s ready to read
            runtime::reactor().register(stream, Interest::READABLE, self.id);
            runtime::reactor().set_waker(waker, self.id); // register the waker for this future in the Reactor
        }

        let mut buff = vec![0u8; 4096];
        loop {
            match self.stream.as_mut().unwrap().read(&mut buff) {
                Ok(0) => {
                    let s = String::from_utf8_lossy(&self.buffer);
                    // Deregister the stream from the Reactor Poll instance, so that we don't get any more events for this stream
                    runtime::reactor().deregister(self.stream.as_mut().unwrap(), self.id);
                    break PollState::Ready(s.to_string());
                }
                Ok(n) => {
                    self.buffer.extend(&buff[0..n]);
                    continue;
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => {
                    // always store the last given Waker, the Waker could have moved to a different Executor between calls
                    runtime::reactor().set_waker(waker, self.id);
                    break PollState::NotReady;
                }

                Err(e) => panic!("{e:?}"),
            }
        }
    }
}
