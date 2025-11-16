use bytes::Bytes;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RespFrame {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Bytes),
    Array(Vec<RespFrame>),
    Null,
}
