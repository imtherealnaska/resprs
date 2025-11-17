use std::collections::HashMap;
use std::result;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::net::{TcpListener, TcpStream};

use tokio::io::BufReader;

use crate::resp_frame::RespFrame;

pub type Db = Arc<Mutex<HashMap<Bytes, RedisValue>>>;

mod parser;
mod resp_frame;
pub mod serializer;

pub struct RedisValue {
    data: Bytes,
    expires_at: Option<Instant>,
}

impl RedisValue {
    fn is_expired(&self) -> bool {
        match self.expires_at {
            Some(instant) => instant < Instant::now(),
            _ => false,
        }
    }
}

#[tokio::main]
async fn main() {
    let bind_addr = "127.0.0.1:6380";
    let listener = TcpListener::bind(bind_addr).await.unwrap();

    println!("Echo server listening on {}", bind_addr);

    let db = Arc::new(Mutex::new(HashMap::new()));

    loop {
        let db_clone = db.clone();
        let (stream, _) = listener.accept().await.unwrap();
        tokio::spawn(async move {
            handle_connection(stream, db_clone).await;
        });
    }
}

async fn handle_connection(stream: TcpStream, db: Db) {
    // splitting into read half and write half.
    // serialise frame needs a writer
    // bufreader needs a reader
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    println!("Client connected {:?}", write_half.peer_addr());

    loop {
        let frame_result = parser::parse_frame(&mut reader).await;

        match frame_result {
            Ok(frame) => {
                println!("Received : {:?}", frame);

                // Process the command and get response
                let response = handle_command(frame, db.clone());

                if let Err(e) = serializer::serialize_frame(&mut write_half, response).await {
                    println!("Error writing to client : {}", e);
                    break;
                }
            }

            Err(e) => {
                println!("Error parsing frame or connection closed: {}", e);
                break;
            }
        }
    }
    println!("Client disconnected: {:?}", write_half.peer_addr());
}

fn handle_increment(args: Vec<RespFrame>, db: Db, amount: i64) -> RespFrame {
    if args.len() != 2 {
        return RespFrame::Error("ERR wrong number of arguments".to_string());
    }
    let RespFrame::BulkString(key) = &args[1] else {
        return RespFrame::Error("ERR key is not a BulkString".to_string());
    };

    let mut db_guard = db.lock().unwrap();

    let value_struct = db_guard.entry(key.clone()).or_insert_with(|| RedisValue {
        data: Bytes::from_static(b"0"),
        expires_at: None,
    });

    if value_struct.is_expired() {
        value_struct.data = Bytes::from_static(b"0");
        value_struct.expires_at = None;
    }

    let Ok(data_str) = std::str::from_utf8(&value_struct.data) else {
        return RespFrame::Error("ERR value is not valid UTF-8".to_string());
    };

    let Ok(current_val) = data_str.parse::<i64>() else {
        return RespFrame::Error("ERR value is not an integer".to_string());
    };

    let new_val = current_val.saturating_add(amount);

    value_struct.data = Bytes::from(new_val.to_string());

    RespFrame::Integer(new_val)
}

// https://redis.io/docs/latest/develop/reference/protocol-spec/#client-handshake
fn handle_command(frame: RespFrame, db: Db) -> RespFrame {
    let RespFrame::Array(args) = frame else {
        return RespFrame::Error("ERR command must be an array".to_string());
    };

    if args.is_empty() {
        return RespFrame::Error("ERR empty command".to_string());
    }

    // extract command name
    let command_name = match &args[0] {
        RespFrame::BulkString(bytes) => String::from_utf8_lossy(bytes).to_uppercase(),
        RespFrame::SimpleString(s) => s.to_uppercase(),
        _ => return RespFrame::Error("ERR invalid command format".to_string()),
    };

    match command_name.as_str() {
        "PING" => {
            if args.len() == 1 {
                RespFrame::SimpleString("PONG".to_string())
            } else if args.len() == 2 {
                // Echo back the argument
                args[1].clone()
            } else {
                RespFrame::Error("ERR wrong number of arguments for 'ping' command".to_string())
            }
        }
        "ECHO" => {
            if args.len() == 2 {
                args[1].clone()
            } else {
                RespFrame::Error("ERR wrong number of arguments for 'echo' command".to_string())
            }
        }
        "COMMAND" => {
            //  minimal response to prevent the assertion failure
            RespFrame::Array(vec![])
        }
        "SET" => {
            if args.len() != 3 {
                return RespFrame::Error(
                    "ERR wrong number of argument for 'set' command".to_string(),
                );
            }

            let RespFrame::BulkString(key) = &args[1] else {
                return RespFrame::Error("ERR key is not a BulkString".to_string());
            };

            let RespFrame::BulkString(value) = &args[2] else {
                return RespFrame::Error("ERR value is not BulkString".to_string());
            };

            let mut db_guard = db.lock().unwrap();

            let new_val = RedisValue {
                data: value.clone(),
                expires_at: db_guard.get(key).and_then(|val| val.expires_at),
            };

            db_guard.insert(key.clone(), new_val);

            RespFrame::SimpleString("OK".to_string())
        }
        "GET" => {
            if args.len() != 2 {
                return RespFrame::Error(
                    "ERR wrong number of argument for 'set' command".to_string(),
                );
            }

            let RespFrame::BulkString(key) = &args[1] else {
                return RespFrame::Error("ERR key is not a BulkString".to_string());
            };

            let mut db_guard = db.lock().unwrap();

            match db_guard.get(key) {
                Some(value) => {
                    if value.is_expired() {
                        db_guard.remove(key);
                        RespFrame::Null
                    } else {
                        RespFrame::BulkString(value.data.clone())
                    }
                }
                None => RespFrame::Null,
            }
        }
        "DEL" => {
            if args.len() < 2 {
                return RespFrame::Error("DEL must have atleast one key to be deleted".to_string());
            }

            let mut db_guard = db.lock().unwrap();
            let mut deleted_count = 0;

            for key_frame in &args[1..] {
                if let RespFrame::BulkString(s) = key_frame
                    && db_guard.remove(s).is_some()
                {
                    deleted_count += 1;
                }
                // DEL ignores keys that arent in the db or are not bulkstring
            }
            RespFrame::Integer(deleted_count)
        }
        "EXISTS" => {
            if args.len() < 2 {
                return RespFrame::Error(
                    "EXISTS must have atleast one key to be checked".to_string(),
                );
            }

            let db_guard = db.lock().unwrap();
            let mut exists_count = 0;

            for key_frame in &args[1..] {
                if let RespFrame::BulkString(s) = key_frame
                    && db_guard.contains_key(s)
                {
                    exists_count += 1;
                }
                // EXISTS ignores keys that arent in the db or are not bulkstring
            }
            RespFrame::Integer(exists_count)
        }
        "EXPIRE" => {
            if args.len() != 3 {
                return RespFrame::Error(
                    "ERR wrong number of arguments for 'expire' command".to_string(),
                );
            }
            //should be BulkString
            let RespFrame::BulkString(key) = &args[1] else {
                return RespFrame::Error("ERR key is not a BulkString".to_string());
            };
            let RespFrame::BulkString(seconds_bytes) = &args[2] else {
                return RespFrame::Error("ERR expiry is not a BulkString".to_string());
            };

            // should be seconds
            let Ok(seconds_str) = std::str::from_utf8(seconds_bytes) else {
                return RespFrame::Error("ERR expiry is not valid UTF-8".to_string());
            };
            let Ok(seconds) = seconds_str.parse::<i64>() else {
                return RespFrame::Error("ERR expiry is not a valid integer".to_string());
            };

            let mut db_guard = db.lock().unwrap();

            if let Some(value) = db_guard.get_mut(key) {
                if seconds <= 0 {
                    db_guard.remove(key);
                    RespFrame::Integer(1)
                } else {
                    let duration = Duration::from_secs(seconds as u64);
                    value.expires_at = Some(Instant::now() + duration);
                    RespFrame::Integer(1)
                }
            } else {
                RespFrame::Integer(0)
            }
        }
        // -2 => key doesnt exst
        // -1 => no expiry set
        // time => time to expiry
        "TTL" => {
            if args.len() != 2 {
                return RespFrame::Error(
                    "ERR wrong number of arguments for 'ttl' command".to_string(),
                );
            }
            let RespFrame::BulkString(key) = &args[1] else {
                return RespFrame::Error("ERR key is not a BulkString".to_string());
            };

            let mut db_guard = db.lock().unwrap();

            match db_guard.get(key) {
                Some(value) => {
                    if value.is_expired() {
                        db_guard.remove(key);
                        RespFrame::Integer(-2)
                    } else {
                        match value.expires_at {
                            Some(instant) => {
                                let remaining = instant.saturating_duration_since(Instant::now());
                                RespFrame::Integer(remaining.as_secs() as i64)
                            }
                            None => RespFrame::Integer(-1),
                        }
                    }
                }
                None => RespFrame::Integer(-2),
            }
        }
        "INCR" => handle_increment(args, db, 1),
        "DECR" => handle_increment(args, db, -1),
        "KEYS" => {
            if args.len() != 2 {
                return RespFrame::Error(
                    "ERR wrong number of arguments for 'keys' command".to_string(),
                );
            }
            let RespFrame::BulkString(pattern_bytes) = &args[1] else {
                return RespFrame::Error("ERR pattern is not a BulkString".to_string());
            };

            if &pattern_bytes[..] != b"*" {
                return RespFrame::Error("ERR only '*' pattern is supported".to_string());
            }

            let mut db_guard = db.lock().unwrap();
            let mut valid_keys = Vec::new();
            let mut keys_to_evict = Vec::new();

            for (key, value) in db_guard.iter() {
                if value.is_expired() {
                    keys_to_evict.push(key.clone());
                } else {
                    valid_keys.push(RespFrame::BulkString(key.clone()));
                }
            }

            for key in keys_to_evict {
                db_guard.remove(&key);
            }

            RespFrame::Array(valid_keys)
        }
        "MSET" => {
            if args.len() < 3 || args.len() % 2 == 0 {
                return RespFrame::Error(
                    "ERR wrong number of arguments for 'mset' command".to_string(),
                );
            }

            let mut db_guard = db.lock().unwrap();

            for pair in args[1..].chunks_exact(2) {
                let (key_frame, val_frame) = (&pair[0], &pair[1]);

                if let (RespFrame::BulkString(key), RespFrame::BulkString(value)) =
                    (key_frame, val_frame)
                {
                    let new_val = RedisValue {
                        data: value.clone(),
                        expires_at: None,
                    };

                    db_guard.insert(key.clone(), new_val);
                }
            }
            RespFrame::SimpleString("OK".to_string())
        }

        "MGET" => {
            if args.len() < 2 {
                return RespFrame::Error("ERR wrong number of args for 'mget".to_string());
            }

            let mut db_guard = db.lock().unwrap();
            let mut results = Vec::with_capacity(args.len() - 1);
            let mut keys_to_evict = Vec::new();

            for key_frame in &args[1..] {
                if let RespFrame::BulkString(key) = key_frame {
                    match db_guard.get(key) {
                        Some(value) => {
                            if value.is_expired() {
                                keys_to_evict.push(key);
                                results.push(RespFrame::Null);
                            } else {
                                results.push(resp_frame::RespFrame::BulkString(value.data.clone()));
                            }
                        }
                        None => {
                            results.push(RespFrame::Null);
                        }
                    }
                } else {
                    results.push(RespFrame::Null);
                }
            }

            for key in keys_to_evict {
                db_guard.remove(key);
            }

            RespFrame::Array(results)
        }
        "STRLEN" => {
            if args.len() != 2 {
                return RespFrame::Error(
                    "ERR wrong number of arguments for 'strlen' command".to_string(),
                );
            }
            let RespFrame::BulkString(key) = &args[1] else {
                return RespFrame::Error("ERR key is not a BulkString".to_string());
            };

            let mut db_guard = db.lock().unwrap();

            match db_guard.get(key) {
                Some(value) => {
                    if value.is_expired() {
                        db_guard.remove(key);
                        RespFrame::Integer(0)
                    } else {
                        RespFrame::Integer(value.data.len() as i64)
                    }
                }
                None => RespFrame::Integer(0),
            }
        }
        "STRLEN" => {
            if args.len() != 3 {
                return RespFrame::Error("ERR wrong number of arguments for 'append' ".to_string());
            }
            let RespFrame::BulkString(key) = &args[1] else {
                return RespFrame::Error("ERR key is not a BulkString".to_string());
            };
            let RespFrame::BulkString(value_to_append) = &args[2] else {
                return RespFrame::Error("ERR value is not a BulkString".to_string());
            };

            let mut db_guard = db.lock().unwrap();

            let value_struct = db_guard.entry(key.clone()).or_insert_with(|| RedisValue {
                data: Bytes::new(),
                expires_at: None,
            });

            if value_struct.is_expired() {
                value_struct.data = Bytes::new();
                value_struct.expires_at = None;
            }

            // bytes is immutable so copy get a vec then extend then get a bytes again
            let mut new_data_vec = value_struct.data.to_vec();
            new_data_vec.extend_from_slice(value_to_append);
            let new_len = new_data_vec.len();

            value_struct.data = Bytes::from(new_data_vec);

            RespFrame::Integer(new_len as i64)
        }
        "GETSET" => {
            if args.len() != 3 {
                return RespFrame::Error(
                    "ERR wrong number of arguments for 'getset' command".to_string(),
                );
            }
            let RespFrame::BulkString(key) = &args[1] else {
                return RespFrame::Error("ERR key is not a BulkString".to_string());
            };
            let RespFrame::BulkString(new_value) = &args[2] else {
                return RespFrame::Error("ERR value is not a BulkString".to_string());
            };

            let mut db_guard = db.lock().unwrap();

            let new_redis_value = RedisValue {
                data: new_value.clone(),
                expires_at: None,
            };

            let old_value_opt = db_guard.insert(key.clone(), new_redis_value);

            match old_value_opt {
                Some(old_value) => {
                    if old_value.is_expired() {
                        RespFrame::Null
                    } else {
                        RespFrame::BulkString(old_value.data.clone())
                    }
                }
                None => RespFrame::Null,
            }
        }
        _ => RespFrame::Error(format!("ERR unknown command '{}'", command_name)),
    }
}
