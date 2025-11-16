use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;

use crate::resp_frame::RespFrame;

pub async fn serialize_frame<W>(stream: &mut W, frame: RespFrame) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    match frame {
        RespFrame::SimpleString(s) => {
            stream.write_all(b"+").await?;
            stream.write_all(s.as_bytes()).await?;
            stream.write_all(b"\r\n").await?;
        }
        RespFrame::Error(e) => {
            stream.write_all(b"-").await?;
            stream.write_all(e.as_bytes()).await?;
            stream.write_all(b"\r\n").await?;
        }
        RespFrame::Integer(i) => {
            stream.write_all(b":").await?;
            stream.write_all(i.to_string().as_bytes()).await?;
            stream.write_all(b"\r\n").await?;
        }
        RespFrame::BulkString(bytes) => {
            let len = bytes.len();
            stream.write_all(b"$").await?;
            stream.write_all(len.to_string().as_bytes()).await?;
            stream.write_all(b"\r\n").await?;

            stream.write_all(&bytes).await?;
            stream.write_all(b"\r\n").await?;
        }
        RespFrame::Array(resp_frames) => {
            stream.write_all(b"*").await?;
            stream
                .write_all(resp_frames.len().to_string().as_bytes())
                .await?;
            stream.write_all(b"\r\n").await?;

            for frame in resp_frames {
                Box::pin(serialize_frame(stream, frame)).await?;
            }
        }
        RespFrame::Null => {
            stream.write_all(b"$-1\r\n").await?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use crate::{resp_frame::RespFrame, serializer::serialize_frame};

    #[tokio::test]
    async fn test_serialize_complex_array() {
        let frame = RespFrame::Array(vec![
            RespFrame::SimpleString("OK".to_string()),
            RespFrame::BulkString(Bytes::from("foobar")),
        ]);

        let mut buf = Vec::new();

        let result = serialize_frame(&mut buf, frame).await;

        assert!(result.is_ok());

        let expected = b"*2\r\n+OK\r\n$6\r\nfoobar\r\n";
        assert_eq!(buf, expected);
    }

    #[tokio::test]
    async fn test_serialize_null() {
        let frame = RespFrame::Null;
        let mut buf = Vec::new();
        let result = serialize_frame(&mut buf, frame).await;

        assert!(result.is_ok());
        assert_eq!(buf, b"$-1\r\n");
    }
}
