use tokio::io::BufReader;

mod parser;
pub mod serializer;
mod resp_frame;

#[tokio::main]
async fn main() {
    let mock_data = b"+OK\r\n:1000\r\n-Error message\r\n";
    let mut reader = BufReader::new(&mock_data[..]);


    println!("Parsing 'OK'...");
    let frame1 = parser::parse_frame(&mut reader).await.unwrap();
    println!("{:?}", frame1); // Should be SimpleString("OK")

    println!("Parsing '1000'...");
    let frame2 = parser::parse_frame(&mut reader).await.unwrap();
    println!("{:?}", frame2); // Should be Integer(1000)

    println!("Parsing 'Error message'...");
    let frame3 = parser::parse_frame(&mut reader).await.unwrap();
    println!("{:?}", frame3); // Should be Error("Error message")
}
