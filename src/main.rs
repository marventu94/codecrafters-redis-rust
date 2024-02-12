use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    for stream in listener.incoming() {
        match stream {
            Ok(_stream) => {
                println!("accepted new connection"); 
                thread::spawn(move || {
                    handle_client(_stream);
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn handle_client(mut stream: TcpStream) {
    let mut buf = [0; 512];

    // read from the stream
    loop {
        let bytes_read = stream.read(&mut buf).expect("Failed to read from client");
        if bytes_read == 0 {
            return;
        }
        stream.write_all(b"+PONG\r\n").expect("Failed to write to client");
    }
}

