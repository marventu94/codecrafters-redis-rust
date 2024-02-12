use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::{thread, str};

fn main() -> anyhow::Result<()> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    for stream in listener.incoming() {
        match stream {
            Ok(_stream) => {
                println!("accepted new connection"); 
                thread::spawn(move || {
                    let _ = handle_client(_stream);
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
    Ok(())
}

fn handle_client(mut stream: TcpStream) -> anyhow::Result<()> {
    let mut buf = [0; 512];

    loop {
        let bytes_read = stream.read(&mut buf).expect("Failed to read from client");
        if bytes_read == 0 {
            return Ok(());
        }
        let s = str::from_utf8(&buf[..bytes_read])?;
        let parts = s.split("\r\n").collect::<Vec<_>>();

        print!("{:?}", parts);

        if parts.len() >= 3 && parts[0].starts_with('*') {
            match parts[2].to_ascii_lowercase().as_ref() {
                "ping" => {
                    let response = "+PONG\r\n";
                    stream.write_all(response.as_bytes())?;
                    stream.flush()?;
                }
                "echo" if parts.len() >= 5 => {
                    let response = format!("${}\r\n{}\r\n", parts[4].len(), parts[4]);
                    stream.write_all(response.as_bytes())?;
                    stream.flush()?;
                }
                _ => {
                    // Response with null
                    stream.write_all("-1\r\n".as_bytes())?;
                    stream.flush()?
                }
            }
        }
    }
}

