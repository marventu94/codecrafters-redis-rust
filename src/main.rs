use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::{thread, str};
use std::time::{Duration, SystemTime};


struct Database {
    db: HashMap<String, (String, Option<SystemTime>)>,
}

impl Database {
    fn new() -> Database {
        Database { db: HashMap::new() }    
    }

    fn get(&self, key: &str) -> Option<&(String, Option<SystemTime>)> {
        self.db.get(key)
    }

    fn set(&mut self, key: &str, value: (&str, Option<SystemTime>)) -> Option<(String, Option<SystemTime>)> {
        self.db.insert(key.to_owned(), (value.0.to_owned(), value.1))
    }
}


fn main() -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    let db = Arc::new(Mutex::new(Database::new()));
    
    for stream in listener.incoming() {
        match stream {
            Ok(_stream) => {
                println!("accepted new connection"); 
                let db_clone = Arc::clone(&db);
                thread::spawn(move || {
                    let _ = handle_client(_stream, db_clone);
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
    Ok(())
}

fn handle_client(mut stream: TcpStream, db: Arc<Mutex<Database>>) -> anyhow::Result<()> {
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
                "set" if parts.len() >= 6 => {
                    if parts[8] == "px" {
                        let key = parts[4];
                        let value = parts[6];
                        // expiry
                        let expiry = parts[8];
                        let millis = expiry.parse().expect("You didn't provde a time for the px parameter");
                        let exp_time = SystemTime::now() + Duration::from_millis(millis);
                        db.lock().expect("Failed to lock").set(key, (value, Some(exp_time)));
                    }else{
                        let key = parts[4];
                        let value = parts[6];
                        db.lock().expect("Failed to lock").set(key, (value, Option::None));
                    }
                    let reply = format!("${}\r\n{}\r\n", "OK".len(), "OK");
                    stream.write_all(reply.as_bytes()).unwrap();
                }
                "get" => {
                    let key = parts[4];
                    match db.lock().expect("Failed to lock").get(key) {
                        Some(reply) => {
                            if let Some(exp_time) = reply.1 {
                                let time = SystemTime::now();
                                if time > exp_time {
                                    stream.write_all(b"$-1\r\n").unwrap();
                                } else {
                                    stream.write_all(format!("+{}\r\n", reply.0.to_string()).as_bytes()).unwrap();
                                }
                            } else {
                                stream.write_all(format!("+{}\r\n", reply.0.to_string()).as_bytes()).unwrap();
                            }
                        }
                        None => {
                            let reply = format!("${}\r\n{}\r\n", "(nil)".len(), "(nil)");
                            stream.write_all(reply.as_bytes()).unwrap();
                        }
                    };
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

