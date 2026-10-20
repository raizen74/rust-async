use std::{
    collections::HashSet,
    io::{self, Read, Result, Write},
    net::TcpStream,
};

use ffi::Event;
use poll::Poll;

mod ffi;
mod poll;

/// Not the entire url, but everyhing after the domain addr
/// i.e. http://localhost/1000/hello => /1000/hello
fn get_req(path: &str) -> String {
    format!(
        "GET {path} HTTP/1.1\r\n\
            Host: localhost\r\n\
            Connection: close\r\n\
            \r\n"
    )
}

fn handle_events(
    events: &[Event],
    streams: &mut [TcpStream],
    handled: &mut HashSet<usize>,
) -> Result<usize> {
    let mut handled_events = 0;
    println!("{events:#?}"); // Event.events = 1 for readable
    // for each event, read the data from the corresponding TcpStream until EOF
    for event in events {
        let index = event.token(); // Event.epoll_data = token to identify the event (the index of the TcpStream in the streams vector)
        println!("event {index} notified");
        let mut data = vec![0u8; 4096];

        loop {
            match streams[index].read(&mut data) {
                Ok(0) => {
                    println!("event {index} drained (EOF)");
                    if !handled.insert(index) {
                        break;
                    }
                    handled_events += 1;
                    break;
                }
                Ok(n) => {
                    let txt = String::from_utf8_lossy(&data[..n]);

                    println!("RECEIVED: {:?}", event);
                    println!("{txt}\n------\n");
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                // this was not in the book example, but it's a error condition
                // you probably want to handle in some way (either by breaking
                // out of the loop or trying a new read call immediately)
                Err(e) if e.kind() == io::ErrorKind::Interrupted => break,
                Err(e) => return Err(e),
            }
        }
    }

    Ok(handled_events)
}

fn main() -> Result<()> {
    let mut poll = Poll::new()?; // creates an epoll instance and returns a Poll struct with a Registry
    let n_events = 5;

    let mut streams = vec![];

    let addr = "localhost:8080".to_string();

    for i in 0..n_events {
        let delay = (n_events - i) * 1000;
        let url_path = format!("/{delay}/request-{i}");
        let request = get_req(&url_path);
        let mut stream = std::net::TcpStream::connect(&addr)?;
        stream.set_nonblocking(true)?;

        stream.write_all(request.as_bytes())?;
        // Register interests for the stream with the epoll instance.
        // The token will be included in the event returned by epoll_wait when the stream is ready to read.
        poll.registry()
            .register(&stream, i, ffi::EPOLLIN | ffi::EPOLLET)?;

        streams.push(stream);
    }

    let mut handled_ids = HashSet::new();

    let mut handled_events = 0;
    while handled_events < n_events {
        let mut events = Vec::with_capacity(10);
        // blocks until an event occurs in the epoll instance or the timeout expires
        // mutates the events vector to contain the events that occurred
        // Event.events = 1 for readable, 2 for writable, etc.
        // Event.epoll_data = token to identify the event (the index of the TcpStream in the streams vector)
        poll.poll(&mut events, None)?;

        if events.is_empty() {
            println!("TIMEOUT (OR SPURIOUS EVENT NOTIFICATION)");
            continue;
        }

        handled_events += handle_events(&events, &mut streams, &mut handled_ids)?;
    }

    println!("FINISHED");
    Ok(())
}
