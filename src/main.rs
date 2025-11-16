use tokio::net::{TcpListener, TcpStream};

use tokio::io::BufReader;

mod parser;
mod resp_frame;
pub mod serializer;

#[tokio::main]
async fn main() {
    let bind_addr = "127.0.0.1:6380";
    let listener = TcpListener::bind(bind_addr).await.unwrap();

    println!("Echo server listening on {}", bind_addr);

    loop {
        let (mut stream, _) = listener.accept().await.unwrap();
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

                if let Err(e) = serializer::serialize_frame(&mut write_half, frame).await {
                    println!("Error writing toclient : {}", e);
                    break;
                }
            }

            Err(e) => {
                println!("Error parsing frame or connectiong closed: {}", e);
                break;
            }
        }
    }
    println!("Client disconnected: {:?}", write_half.peer_addr());
}
