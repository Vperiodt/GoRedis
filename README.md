# Go-Redis

This project is a Redis-compatible server implemented in Go. 

## Features
Supports a wide array of commands and features, grouped by functionality:

1. RDB Persistence
2. Transactions
3. Streams (XADD, XRANGE, XREAD)
4. Lists (LPUSH, RPUSH, LRANGE, etc.)
5. Replication


## Getting Started

### Prerequisites
- Go (version 1.18 or newer recommended).
- `redis-cli` (for interacting with the server).

### Building the Server
1. Clone the repository.
2. Make the build script executable:
   ```sh
   chmod +x build.sh
   ```
3. Run the build script:
   ```sh
   ./build.sh
   ```
   This will create an executable at `./bin/go-redis`.

### Running the Server

You can run the server as a master or a replica.

**To run as a master:**
```sh
./bin/go-redis --port 6379
```

**To run as a replica of a master running on port 6379:**
```sh
./bin/go-redis --port 6380 --replicaof localhost 6379
```

### Connecting with a Client

You can use `redis-cli` to connect to your running server instance.

```sh
# Connect to a server on the default port
redis-cli

# Connect to a server on a specific port
redis-cli -p 6380

# Send some commands
127.0.0.1:6379> PING
PONG

127.0.0.1:6379> SET name "your-name"
OK

127.0.0.1:6379> GET name
"your-name"

127.0.0.1:6379> XADD mystream * sensor-id 1234 temperature 19.8
"1726434915357-0"
```
