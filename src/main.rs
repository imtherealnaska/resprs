use tokio::net::{TcpListener, TcpStream};

use tokio::io::BufReader;

use crate::resp_frame::RespFrame;

mod parser;
mod resp_frame;
pub mod serializer;

#[tokio::main]
async fn main() {
    let bind_addr = "127.0.0.1:6380";
    let listener = TcpListener::bind(bind_addr).await.unwrap();

    println!("Echo server listening on {}", bind_addr);

    loop {
        let (stream, _) = listener.accept().await.unwrap();
        tokio::spawn(async move {
            handle_connection(stream).await;
        });
    }
}

async fn handle_connection(stream: TcpStream) {
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
                let response = handle_command(frame);

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

// https://redis.io/docs/latest/develop/reference/protocol-spec/#client-handshake
fn handle_command(frame: RespFrame) -> RespFrame {
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
        _ => RespFrame::Error(format!("ERR unknown command '{}'", command_name)),
    }
}
