### Project Title

RESP Protocol Implementation

- Redis-compatible server.

### Technical Highlights

- Complete RESP (Redis Serialization Protocol) parser and serializer
- 18 Redis commands implemented
- Thread-safe in-memory storage
- Expiration system with background cleanup
- Compatible with standard redis-cli

### Implemented Commands

**Basic:**

- PING, ECHO

**Storage:**

- GET, SET, DEL, MGET, MSET, GETSET

**Key Management:**

- EXISTS, KEYS

**Counters:**

- INCR, DECR, INCRBY, DECRBY

**String Operations:**

- APPEND, STRLEN

**Expiration:**

- EXPIRE, TTL

### Architecture

- RESP Protocol Layer: Parse and serialize all 5 RESP types
- Command Parser: Extract commands from RESP arrays
- Storage Engine: Thread-safe HashMap with expiration metadata
- Expiration Manager: Background cleanup of expired keys
- TCP Server: Async connection handling with Tokio

### Example Usage

```bash
# Start server
cargo run

# Connect with redis-cli
redis-cli -p 6380
> SET mykey "hello"
OK
> GET mykey
"hello"
> INCR counter
(integer) 1
> EXPIRE counter 60
(integer) 1
> TTL counter
(integer) 59
```

### Technical Challenges Solved

- RESP protocol parsing (handling all 5 types: Simple String, Error, Integer, Bulk String, Array)
- Thread-safe concurrent access to shared storage
- Atomic counter operations (INCR/DECR)
- Expiration timing and cleanup without blocking operations
- Command argument validation and error handling

### Tech Stack

- Rust
- Tokio (async runtime)
- Arc<Mutex<HashMap>> (thread-safe storage)
- Custom RESP parser/serializer

### Compatibility

- Works with official redis-cli
- Compatible with Redis protocol clients
- Subset of Redis functionality
