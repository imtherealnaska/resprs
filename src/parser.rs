use bytes::Bytes;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, BufReader};

use crate::resp_frame::RespFrame;

pub async fn parse_frame<R>(stream: &mut BufReader<R>) -> std::io::Result<RespFrame>
where
    R: AsyncRead + Unpin,
{
    let prefix = stream.read_u8().await?;

    match prefix {
        b'+' => parse_simple_string(stream).await,
        b'-' => parse_error(stream).await,
        b':' => parse_integer(stream).await,
        b'$' => parse_bulk_string(stream).await,
        b'*' => Box::pin(parse_array(stream)).await,
        _ => {
            println!(
                "[parser] Received unkown prefix: {} (char : {})",
                prefix, prefix as char
            );
            Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Unkown RESP frame prefix",
            ))
        }
    }
}

async fn parse_array<R>(stream: &mut BufReader<R>) -> std::io::Result<RespFrame>
where
    R: AsyncRead + Unpin,
{
    let line = read_line_as_string(stream).await?;

    let length: i64 = line.parse().map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Could not parse array length",
        )
    })?;

    if length == -1 {
        return Ok(RespFrame::Null);
    }

    let length_usize = length as usize;
    let mut elements = Vec::with_capacity(length_usize);

    for _ in 0..length_usize {
        let element_frame: RespFrame = parse_frame(stream).await?;
        elements.push(element_frame);
    }

    Ok(RespFrame::Array(elements))
}

async fn parse_bulk_string<R>(stream: &mut BufReader<R>) -> std::io::Result<RespFrame>
where
    R: AsyncRead + Unpin,
{
    let line = read_line_as_string(stream).await?;

    let length: i64 = line.parse().map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Could not parse bulk string",
        )
    })?;

    if length == -1 {
        return Ok(RespFrame::Null);
    }

    let mut data_buf = vec![0; length as usize];

    stream.read_exact(&mut data_buf).await?;

    let mut crlf_buf = [0; 2];
    stream.read_exact(&mut crlf_buf).await?;

    if crlf_buf != *b"\r\n" {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Bulk string did not end with \r\n",
        ));
    }

    Ok(RespFrame::BulkString(Bytes::from(data_buf)))
}

async fn parse_error<R>(stream: &mut BufReader<R>) -> std::io::Result<RespFrame>
where
    R: AsyncRead + Unpin,
{
    let line = read_line_as_string(stream).await?;
    Ok(RespFrame::Error(line))
}

async fn read_line_as_string<R>(stream: &mut BufReader<R>) -> std::io::Result<String>
where
    R: AsyncRead + Unpin,
{
    let mut line_buf = Vec::new();
    let len = stream.read_until(b'\n', &mut line_buf).await?;
    if len < 2 || line_buf[len - 2] != b'\r' {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Simple string did not end with \\r\n",
        ));
    }

    let s = String::from_utf8(line_buf[..len - 2].to_vec())
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;

    Ok(s)
}

async fn parse_integer<R>(stream: &mut BufReader<R>) -> std::io::Result<RespFrame>
where
    R: AsyncRead + Unpin,
{
    let line = read_line_as_string(stream).await?;

    let val: i64 = line.parse().map_err(|_| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "Could not parse integer")
    })?;

    Ok(RespFrame::Integer(val))
}

async fn parse_simple_string<R>(stream: &mut BufReader<R>) -> std::io::Result<RespFrame>
where
    R: AsyncRead + Unpin,
{
    let line = read_line_as_string(stream).await?;
    Ok(RespFrame::SimpleString(line))
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use tokio::io::BufReader;

    use crate::{parser::parse_frame, resp_frame::RespFrame};

    #[tokio::test]
    async fn test_parse_simple_string() {
        let input_bytes = b"+OK\r\n";

        let mut reader = BufReader::new(&input_bytes[..]);
        let result = parse_frame(&mut reader).await;

        assert!(result.is_ok());

        let frame = result.unwrap();
        assert_eq!(frame, RespFrame::SimpleString("OK".to_string()));
    }

    #[tokio::test]
    async fn test_parse_error() {
        let input_bytes = b"-ERR unknown command\r\n";
        let mut reader = BufReader::new(&input_bytes[..]);

        let result = parse_frame(&mut reader).await;

        assert!(result.is_ok());
        let frame = result.unwrap();
        assert_eq!(frame, RespFrame::Error("ERR unknown command".to_string()));
    }

    #[tokio::test]
    async fn test_parse_integer() {
        let input_bytes = b":1024\r\n";
        let mut reader = BufReader::new(&input_bytes[..]);

        let result = parse_frame(&mut reader).await;

        assert!(result.is_ok());
        let frame = result.unwrap();
        assert_eq!(frame, RespFrame::Integer(1024));
    }

    #[tokio::test]
    async fn test_parse_negative_integer() {
        let input_bytes = b":-55\r\n";
        let mut reader = BufReader::new(&input_bytes[..]);

        let result = parse_frame(&mut reader).await;

        assert!(result.is_ok());
        let frame = result.unwrap();
        assert_eq!(frame, RespFrame::Integer(-55));
    }

    #[tokio::test]
    async fn test_parse_bulk_string() {
        let input_bytes = b"$6\r\nfoobar\r\n";
        let mut reader = BufReader::new(&input_bytes[..]);

        let result = parse_frame(&mut reader).await;

        assert!(result.is_ok());
        let frame = result.unwrap();

        let expected = RespFrame::BulkString(Bytes::from("foobar"));
        assert_eq!(frame, expected);
    }

    #[tokio::test]
    async fn test_parse_array() {
        let input_bytes = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
        let mut reader = BufReader::new(&input_bytes[..]);

        let result = parse_frame(&mut reader).await;

        assert!(result.is_ok());
        let frame = result.unwrap();

        let expected = RespFrame::Array(vec![
            RespFrame::BulkString(Bytes::from("SET")),
            RespFrame::BulkString(Bytes::from("key")),
            RespFrame::BulkString(Bytes::from("value")),
        ]);

        assert_eq!(frame, expected);
    }

    #[tokio::test]
    async fn test_parse_empty_array() {
        let input_bytes = b"*0\r\n";
        let mut reader = BufReader::new(&input_bytes[..]);

        let result = parse_frame(&mut reader).await;

        assert!(result.is_ok());
        let frame = result.unwrap();

        let expected = RespFrame::Array(vec![]);
        assert_eq!(frame, expected);
    }

    #[tokio::test]
    async fn test_parse_null_array() {
        let input_bytes = b"*-1\r\n";
        let mut reader = BufReader::new(&input_bytes[..]);

        let result = parse_frame(&mut reader).await;

        assert!(result.is_ok());
        let frame = result.unwrap();
        assert_eq!(frame, RespFrame::Null);
    }
}
