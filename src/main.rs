use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::{thread, str};
use std::env;
use std::time::{Duration, SystemTime};

struct Server {
    db: Database,
    replica_of: Option<(String, String)>,
}

impl Server {
    fn new() -> Server {
        Server {
            db: Database::new(),
            replica_of: None,
        }
    }
    fn as_replica_of(&self, host: String, port: String) -> Server {
        Server {
            db: Database::new(),
            replica_of: Some((host, port)),
        }
    }
}

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



fn parse_cli_port() -> Option<u16> {
    let index = env::args().position(|x| x == "--port")?;
    let value = env::args().nth(index + 1)?;
    value.parse().ok()
}


fn main() -> anyhow::Result<()> {
    let server_port = parse_cli_port().unwrap_or(6379);
    let listener = TcpListener::bind(("127.0.0.1", server_port)).unwrap();

    // --replicaof <host> <port>
    let mut replica_of: Option<(String, String)> = None;
    if let Some(index) = env::args().position(|arg| arg == "--replicaof") {
        if env::args().len() < index + 3 {
            panic!("--replicaof requires 2 arguments");
        }
        let host = env::args().nth(index + 1).clone().unwrap();
        let port = env::args().nth(index + 2).clone().unwrap();
        replica_of = Some((host, port));
    }

    let server: Arc<Mutex<Server>> = if let Some((host, port)) = replica_of {
        let url = &format!("{}:{}", host, port);
        let mut remote_stream = TcpStream::connect(url).unwrap();

        handle_ping_command_slave(&mut remote_stream);
        handle_replconf_command_slave(&mut remote_stream, server_port.to_string());
        handle_psync_command_slave(&mut remote_stream);

        Arc::new(Mutex::new(Server::new().as_replica_of(host, port)))
    } else {
        Arc::new(Mutex::new(Server::new()))
    };
    
    for stream in listener.incoming() {
        match stream {
            Ok(_stream) => {
                println!("accepted new connection"); 
                let server_clone = Arc::clone(&server);
                thread::spawn(move || {
                    let _ = handle_client(_stream, server_clone);
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
    Ok(())
}

fn handle_client(mut stream: TcpStream, server: Arc<Mutex<Server>>) -> anyhow::Result<()> {
    let mut buf = [0; 512];

    loop {
        let bytes_read = stream.read(&mut buf).expect("Failed to read from client");
        if bytes_read == 0 {
            return Ok(());
        }
        let s = str::from_utf8(&buf[..bytes_read])?;
        let parts = s.split("\r\n").collect::<Vec<_>>();

        println!("{:?}", parts);

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
                    if parts.len() > 8 && parts[8] == "px" {
                        let key = parts[4];
                        let value = parts[6];
                        // expiry
                        let expiry = parts[10];
                        let millis = expiry.parse().expect("You didn't provde a time for the px parameter");
                        let exp_time = SystemTime::now() + Duration::from_millis(millis);
                        
                        server.lock().unwrap().db.set(key, (value, Some(exp_time)));
                    }else{
                        let key = parts[4];
                        let value = parts[6];
                        server.lock().unwrap().db.set(key, (value, Option::None));
                    }
                    let reply = format!("${}\r\n{}\r\n", "OK".len(), "OK");
                    stream.write_all(reply.as_bytes()).unwrap();
                }
                "get" => {
                    let key = parts[4];
                    match server.lock().unwrap().db.get(key) {
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
                "info" if parts.len() >= 5 && parts[4] == "replication" => {
                    let mut response = String::new();
                    if server.lock().unwrap().replica_of == None {
                        response.push_str(&format!("role:master\r\n"));
                    } else {
                        response.push_str(&format!("role:slave\r\n"));
                    }
                    response.push_str(&format!("master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\r\n"));
                    response.push_str(&format!("master_repl_offset:0\r\n"));
                    stream.write_all(format!("${}\r\n{}\r\n", response.len(), response).as_bytes())?;
                    stream.flush()?
                }
                "replconf" => {
                    let _ = stream.write_all(b"+OK\r\n");
                    stream.flush()?
                },
                "PSYNC" => {
                    let msg = "+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0";
                    let msg_reso = format!("${}\r\n{}\r\n", msg.len(), msg);
                    let _ = stream.write_all(msg_reso.as_bytes());
                    stream.flush()?
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

fn handle_ping_command_slave(stream: &mut TcpStream){
    let msg = b"*1\r\n$4\r\nping\r\n";
    stream.write_all(msg).unwrap();
    let _ = stream.flush();
}

fn handle_replconf_command_slave(stream: &mut TcpStream, port: String){
    let msg1 = format!("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n${}\r\n{}\r\n",port.len(), port);
    let _ = stream.write_all(msg1.as_bytes());
    let _ = stream.flush();
    let msg2 = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
    let _ = stream.write_all(msg2.as_bytes());
}

fn handle_psync_command_slave(stream: &mut TcpStream){
    let msg = b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
    let _ = stream.write_all(msg);
    let _ = stream.flush();
}