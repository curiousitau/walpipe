# PostgreSQL Replication Checker - Rust Edition

A Rust implementation of a PostgreSQL logical replication client that connects to a database, creates replication slots, and displays changes in real-time. This is a Rust port of the original C++ implementation from [https://github.com/fkfk000/replication_checker](https://github.com/fkfk000/replication_checker).

>　https://www.postgresql.org/docs/current/protocol-replication.html

## Features

- **Logical Replication**: Connects to PostgreSQL as a replication client using the logical replication protocol
- **Real-time Change Display**: Shows INSERT, UPDATE, DELETE, TRUNCATE operations as they happen
- **Streaming Transaction Support**: Handles both regular and streaming (large) transactions
- **Built with libpq-sys**: Uses low-level PostgreSQL libpq bindings for maximum performance and control
- **Comprehensive Logging**: Uses tracing for structured logging and debugging
- **Multiple Event Sinks**: Supports HTTP endpoints, Hook0, and STDOUT output

## Prerequisites

- PostgreSQL server with logical replication enabled (`wal_level = logical`)
- A publication created on the source database
- libpq development libraries installed on your system
- Rust 1.70+ with Cargo

### PostgreSQL Setup

1. Enable logical replication in your PostgreSQL configuration:
   ```sql
   ALTER SYSTEM SET wal_level = logical;
   -- Restart PostgreSQL server after this change
   ```

2. Create a publication for the tables you want to replicate:
   ```sql
   CREATE PUBLICATION my_publication FOR TABLE table1, table2;
   -- Or for all tables:
   CREATE PUBLICATION my_publication FOR ALL TABLES;
   ```

3. Create a user with replication privileges:
   ```sql
   CREATE USER replicator WITH REPLICATION LOGIN PASSWORD 'password';
   GRANT SELECT ON ALL TABLES IN SCHEMA public TO replicator;
   ```

## Installation

> [!WARNING]
> Please note : PostgreSQL DB version must equal or higher to version 14, more information refer to below link. 
> * https://www.postgresql.org/docs/14/protocol-replication.html
> * https://www.postgresql.org/docs/current/protocol-logical-replication.html#PROTOCOL-LOGICAL-REPLICATION-PARAMS


### From Source

```bash
git clone <this-repository>
cd pg_replica_rs
cargo build --release
```

### System Dependencies

Make sure you have libpq development libraries installed:

**Ubuntu/Debian:**
```bash
sudo apt-get install libpq-dev
```

**CentOS/RHEL/Fedora:**
```bash
sudo yum install postgresql-devel
# or
sudo dnf install postgresql-devel
```

**macOS:**
```bash
brew install postgresql
```

## Usage

### Basic Usage

```bash
# Set environment variables
export DATABASE_URL="postgresql://user:password@localhost:5432/dbname"
export SLOT_NAME="my_slot"
export PUB_NAME="my_publication"

# Run the replication checker
./target/release/walpipe
```

### Event Sink Examples

#### HTTP Event Sink
```bash
export DATABASE_URL="postgresql://user:password@localhost:5432/dbname"
export EVENT_SINK="http"
export HTTP_ENDPOINT_URL="https://your-webhook-endpoint.com/events"
export SLOT_NAME="my_slot"
export PUB_NAME="my_publication"

./target/release/walpipe
```

#### Hook0 Event Sink
```bash
export DATABASE_URL="postgresql://user:password@localhost:5432/dbname"
export EVENT_SINK="hook0"
export HOOK0_API_URL="https://your-hook0-instance.com"
export HOOK0_APPLICATION_ID="12345678-1234-1234-1234-123456789012"
export HOOK0_API_TOKEN="your-api-token"
export SLOT_NAME="my_slot"
export PUB_NAME="my_publication"

./target/release/walpipe
```

#### STDOUT Event Sink (Default)
```bash
export DATABASE_URL="postgresql://user:password@localhost:5432/dbname"
export SLOT_NAME="my_slot"
export PUB_NAME="my_publication"

# Or explicitly set:
export EVENT_SINK="stdout"

./target/release/walpipe
```

### Command Line Arguments

The program accepts PostgreSQL connection parameters as space-separated key-value pairs:

```bash
pg_replica_rs user <username> password <password> host <hostname> port <port> dbname <database> [additional_params...]
```

### Environment Variables

#### Core Configuration
- `DATABASE_URL`: PostgreSQL connection string (required)
- `SLOT_NAME`: Name of the replication slot to create/use (default: "sub")
- `PUB_NAME`: Name of the publication to subscribe to (default: "pub")

#### Event Sink Configuration
- `EVENT_SINK`: Event sink type - "http", "hook0", or "stdout" (optional, defaults to "stdout")

##### HTTP Event Sink (when EVENT_SINK=http)
- `HTTP_ENDPOINT_URL`: URL for HTTP event sink (required)

##### Hook0 Event Sink (when EVENT_SINK=hook0)
- `HOOK0_API_URL`: Hook0 API URL (required)
- `HOOK0_APPLICATION_ID`: Hook0 application UUID (required)
- `HOOK0_API_TOKEN`: Hook0 API token (required)

### Logging

Set the `RUST_LOG` environment variable to control logging levels:

```bash
# Info level (default)
RUST_LOG=info ./target/release/pg_replica_rs ...

# Debug level for detailed output
RUST_LOG=debug ./target/release/pg_replica_rs ...

# Only errors
RUST_LOG=error ./target/release/pg_replica_rs ...
```

## Example Output

When running, you'll see output like:

```
2024-01-15T10:30:00.123Z INFO pg_replica_rs: Connection string: user=postgres password=*** host=localhost port=5432 dbname=testdb
2024-01-15T10:30:00.124Z INFO pg_replica_rs: Slot name: my_slot  
2024-01-15T10:30:00.124Z INFO pg_replica_rs: Publication name: my_publication
2024-01-15T10:30:00.125Z INFO pg_replica_rs: Successfully connected to database server
2024-01-15T10:30:00.126Z INFO pg_replica_rs: System identification successful
2024-01-15T10:30:00.127Z INFO pg_replica_rs: Creating replication slot: my_slot
2024-01-15T10:30:00.128Z INFO pg_replica_rs: Started receiving data from database server

BEGIN: Xid 12345
table public.users: INSERT: id: 100 name: John Doe email: john@example.com 
table public.orders: INSERT: id: 50 user_id: 100 amount: 99.99 
COMMIT

BEGIN: Xid 12346
table public.users UPDATE Old REPLICA IDENTITY: id: 100 New Row: id: 100 name: John Smith email: john.smith@example.com 
COMMIT

BEGIN: Xid 12347  
table public.orders: DELETE: (INDEX): id: 50 user_id: 100 
COMMIT
```

## Architecture

The implementation consists of several key modules:

- **`main.rs`**: Application entry point, argument parsing, and async runtime setup
- **`server.rs`**: Main replication server that manages the PostgreSQL connection and message processing
- **`parser.rs`**: Protocol message parser for PostgreSQL logical replication messages
- **`types.rs`**: Data structures for relations, tuples, and replication messages
- **`utils.rs`**: Utility functions for connection management, byte manipulation, and PostgreSQL integration

### Key Components

1. **Connection Management**: Safe wrapper around libpq connections with proper resource cleanup
2. **Protocol Parsing**: Complete implementation of PostgreSQL logical replication protocol
3. **Message Processing**: Handlers for all logical replication message types (BEGIN, COMMIT, INSERT, UPDATE, DELETE, TRUNCATE, etc.)
4. **Feedback System**: Implements the feedback protocol to acknowledge processed WAL positions

### Hook0 sink and `event_history`

The hook0 sink runs a side-channel batch recorder
(`src/events/sink/event_history.rs`) that writes a `DELIVERED` row to
`public.event_history` for every event handed off to hook0. This is
the same `event_history` the application uses — it's an event-sourcing
state-transition log:

```
event_stream (the event)
  └─ event_history (one row per state transition, FK on event_id)
       ├─ status='CREATED'   ← written by event_stream's after-insert trigger
       └─ status='DELIVERED' ← written by walpipe after hook0 dispatch
```

Walpipe's INSERT must use the live schema (`event_id` FK, `status`,
`metadata`, `changed_at` defaults to `now()`). Earlier versions of the
recorder targeted a hypothetical private schema (`event_id PK`,
`event_type`, `source_created_at`, `delivered_at`) that doesn't exist
in this DB; flushes silently failed with `db error` until the SQL was
aligned. Don't reintroduce a private schema — the application reads
the same table to derive delivery state.

### Two PG connections, two TLS stories (gotcha)

Walpipe holds two distinct connections to Postgres:

1. **Replication connection** (`PGConnection` in `utils/connection.rs`)
   uses **libpq** via `libpq-sys`. libpq honours `sslmode=require`
   internally and handles TLS handshake itself.
2. **`event_history` recorder** (`events/sink/event_history.rs`) uses
   **tokio-postgres** (pure Rust). tokio-postgres negotiates TLS but
   needs an *actual TLS implementation* passed in — `NoTls` will fail
   the handshake against any server that requires SSL.

Against managed Postgres (Azure, RDS, etc.) the recorder must be
constructed with `postgres-native-tls::MakeTlsConnector::new(
native_tls::TlsConnector::builder().build()?)`. The runtime container
also needs `ca-certificates` installed so native-tls has a trust
store — see `Dockerfile`. If you ever change the recorder back to
`NoTls` for local-dev convenience, gate it on the conn string instead
of removing the TLS path outright.

## Message Flow Sequence Diagram

The following sequence diagram shows the interaction between PostgreSQL and walpipe during logical replication:

```mermaid
sequenceDiagram
    participant PG as PostgreSQL
    participant WAL2 as walpipe
    participant SINK as Event Sink (HTTP/Hook0/Stdout)

    Note over PG,WAL2: Connection Setup Phase
    WAL2->>PG: CONNECT (connection_string)
    PG-->>WAL2: Connection established
    
    WAL2->>PG: EXEC: SHOW wal_level
    PG-->>WAL2: wal_level = "logical"
    
    WAL2->>PG: EXEC: IDENTIFY_SYSTEM
    PG-->>WAL2: System info (timeline, xlogpos, dbname)
    
    WAL2->>PG: EXEC: SELECT FROM pg_replication_slots
    PG-->>WAL2: Replication slot exists
    
    WAL2->>PG: EXEC: SELECT FROM pg_publication
    PG-->>WAL2: Publication exists
    
    WAL2->>PG: EXEC: START_REPLICATION SLOT slot_name LOGICAL 0/0
    Note over WAL2: proto_version '2', streaming 'on', publication_names 'pub_name'
    PG-->>WAL2: CopyBothResponse mode started

    Note over PG,WAL2: Replication Loop - Periodic Feedback
    loop Every feedback_interval_secs (default 1s)
        WAL2->>WAL2: check_and_send_feedback()
        alt Last feedback > timeout
            WAL2->>PG: FEEDBACK (r)
            Note over WAL2: received_lsn, flushed_lsn, applied_lsn, timestamp
            PG-->>WAL2: Feedback acknowledged
        end
        
        WAL2->>PG: get_copy_data()
        alt No data available
            PG-->>WAL2: None
            WAL2->>WAL2: sleep(10ms)
        end
    end

    Note over PG,WAL2: Keepalive Message Handling
    PG->>WAL2: KEEPALIVE (k)
    Note over PG: log_pos, timestamp, reply_requested
    
    alt reply_requested = 1
        WAL2->>PG: FEEDBACK (r)
        Note over WAL2: Immediate feedback requested
        WAL2->>PG: flush()
        PG-->>WAL2: Feedback processed
    else reply_requested = 0
        Note over WAL2: No immediate response needed
    end
    PG-->>WAL2: Keepalive processed

    Note over PG,WAL2: WAL Data Processing
    PG->>WAL2: WAL DATA (w)
    Note over PG: data_start, wal_data, start_lsn
    
    WAL2->>WAL2: process_wal_message()
    WAL2->>WAL2: Parse logical replication message
    WAL2->>WAL2: process_replication_message()
    
    alt Event sink configured
        WAL2->>SINK: send_event(message)
        SINK-->>WAL2: Event delivered successfully
        WAL2->>WAL2: update_applied_lsn()
    else No event sink
        WAL2->>WAL2: update_applied_lsn() immediately
    end
    
    WAL2->>PG: FEEDBACK (r)
    Note over WAL2: Sent after processing each WAL message
    PG-->>WAL2: Feedback acknowledged
    PG-->>WAL2: WAL message processed

    Note over PG,WAL2: Error Handling
    alt Event sink fails
        WAL2->>WAL2: Log error
        WAL2->>WAL2: Return ReplicationError
        Note over WAL2: Replication loop may terminate
    end
```

### Key Feedback Events

1. **Periodic Feedback**: Sent every `feedback_interval_secs` (default 1 second) to maintain connection health
2. **Requested Feedback**: Sent immediately when PostgreSQL sets `reply_requested=1` in keepalive messages
3. **Post-Processing Feedback**: Sent after each WAL message is successfully processed and delivered to event sink

The feedback mechanism ensures PostgreSQL knows which LSN positions have been received and applied, preventing unnecessary retransmission and maintaining replication consistency.

## Roadmap

### 🚀 Planned Features

- **JSONL STDOUT Formatting**: Output events in JSON Lines format for better machine parsing and integration with log aggregation tools
- **Enhanced Error Recovery**: Implement automatic reconnection and retry logic with exponential backoff
- **Replication Slot Management**: Automatic slot cleanup, status monitoring, and lifecycle management
- **Binary Type Support**: Proper handling of PostgreSQL binary data types (bytea, uuid, etc.)
- **Configuration File Support**: Support for TOML/YAML configuration files in addition to environment variables
- **Metrics and Monitoring**: Prometheus metrics endpoint for monitoring replication health and performance
- **TLS/SSL Support**: Encrypted connections to PostgreSQL and HTTP endpoints
- **Message Filtering**: Configurable filtering of events based on table, operation type, or content
- **Batch Processing**: Batch multiple events for improved HTTP endpoint performance
- **Kubernetes Integration**: Helm charts and Kubernetes deployment manifests

### 🔧 Technical Improvements

- **Memory Optimization**: Reduce memory footprint for high-throughput scenarios
- **Performance Tuning**: Optimize parsing and event processing for lower latency
- **Testing Infrastructure**: Comprehensive integration tests and performance benchmarks
- **Documentation**: API documentation and integration guides

### 📋 Considered Features

- **Multi-Source Replication**: Connect to multiple PostgreSQL databases simultaneously
- **Data Transformation**: Built-in data transformation and mapping capabilities
- **Storage Backends**: Direct integration with databases or message queues
- **Web Dashboard**: Web-based monitoring and management interface

---

## Supported Operations

- ✅ **BEGIN** - Transaction start
- ✅ **COMMIT** - Transaction commit  
- ✅ **INSERT** - Row insertions
- ✅ **UPDATE** - Row updates (with old/new value support)
- ✅ **DELETE** - Row deletions
- ✅ **TRUNCATE** - Table truncation
- ✅ **RELATION** - Table schema information
- ✅ **Streaming Transactions** - Large transaction support
- ✅ **Keep-alive** - Connection health monitoring

## Limitations

- Currently displays changes in a human-readable format rather than processing them
- Only supports text data types (binary types show as raw data)
- Replication slot management is basic (creates slot, doesn't handle cleanup on exit)
- Error recovery is minimal (will exit on most errors)

## Troubleshooting

### Connection Issues

1. **"Connection failed"**: Check your PostgreSQL server is running and accessible
2. **"Permission denied"**: Ensure your user has REPLICATION privilege
3. **"Replication slot creation failed"**: The slot might already exist, or you lack privileges

### Compilation Issues

1. **"libpq not found"**: Install PostgreSQL development libraries
2. **"linking failed"**: Ensure libpq is in your library path

### Runtime Issues

1. **No data received**: Check that your publication includes the tables being modified
2. **"Unknown relation"**: The replication stream may be out of sync; restart the application
3. **Slot falls behind / silent disconnect**: On Azure PostgreSQL (and other managed services), the replication slot can fall behind the WAL retention window. When this happens, walpipe stays connected but stops receiving data — the slot shows `active = f` and no new events are forwarded even though `event_stream` has inserts. The fix is to drop and recreate the slot to reset to the current WAL position:

   ```bash
   # Scale walpipe down first (slot cannot be dropped while active)
   kubectl scale deployment/walpipe --replicas=0

   # Drop and recreate the slot
   psql "postgresql://...?sslmode=require" \
     -c "SELECT pg_drop_replication_slot('walpipe_slot');" \
     -c "SELECT * FROM pg_create_logical_replication_slot('walpipe_slot', 'pgoutput');"

   # Scale back up
   kubectl scale deployment/walpipe --replicas=1
   ```

   After recreating, walpipe will start from the current WAL position and pick up new events. Events that occurred while the slot was behind will be lost (walpipe never received them).

### docker Build and run:

```bash
docker build -t pg_replica_rs .
docker run -e slot_name=my_slot -e pub_name=my_pub pg_replica_rs user postgres password secret host host.docker.internal port 5432 dbname mydb
```

## License

This project is licensed under the same terms as the original C++ implementation.

## Acknowledgments

- Based on the original C++ implementation by [fkfk000](https://github.com/fkfk000/replication_checker)
- Uses the excellent [libpq-sys](https://crates.io/crates/libpq-sys) crate for PostgreSQL integration
- Built with the [Tokio](https://tokio.rs/) async runtime
