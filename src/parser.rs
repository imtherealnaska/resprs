use std::io::Error;

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
        _ => Err(Error::new(
            std::io::ErrorKind::InvalidInput,
            "Unkown RESP frame prefix",
        )),
    }
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
}
