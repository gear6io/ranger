# Unified Protocol Package

This package provides a unified protocol system for Icebox that eliminates protocol mismatches between the SDK and server by providing a single source of truth for all protocol definitions.

## Architecture

The unified protocol system consists of several key components:

### 1. Signal Interface (`types.go`)
- **Signal**: Core interface that all protocol messages implement
- **Message**: Complete protocol message with length, type, and payload
- **Codec**: Interface for encoding/decoding messages
- **Direction**: Indicates whether a signal goes from client to server or vice versa

### 2. Protocol Constants (`constants.go`)
- **Client Signals**: Messages sent from client to server (types 0-4)
- **Server Signals**: Messages sent from server to client (types 100-115)
- **Protocol Versions**: ClickHouse-compatible protocol version constants

### 3. Registry System (`registry.go`)
- **Registry**: Manages client and server signal implementations
- **Client Registry**: Maps client signal types to implementations
- **Server Registry**: Maps server signal types to implementations
- **Signal Info**: Metadata about each signal type

### 4. Codec Implementation (`codec.go`)
- **DefaultCodec**: Implements the Codec interface
- **Message Format**: `[4 bytes length][1 byte type][payload]`
- **Big Endian**: All multi-byte values use big endian encoding
- **Signal Unpacking**: Automatically determines signal direction and unpacks accordingly

### 5. Signal Factory (`factory.go`)
- **SignalFactory**: Creates new signal instances
- **Constructor Registration**: Maps signal types to constructor functions
- **Reflection Fallback**: Uses reflection when constructors aren't registered

### 6. Signal Implementations
- **Client Signals** (`signals/client/`): Implementations of client-to-server messages
- **Server Signals** (`signals/server/`): Implementations of server-to-client messages

## Usage

### Basic Setup

```go
// Create registry and factory
registry := protocol.NewRegistry()
factory := protocol.NewSignalFactory()

// Create codec
codec := protocol.NewDefaultCodec(registry, factory)

// Register signal constructors
factory.RegisterConstructor(protocol.ClientHello, func() protocol.Signal {
    return &client.Hello{}
})

// Register signals with metadata
registry.RegisterClientSignal(&client.Hello{}, &protocol.SignalInfo{
    Type:      protocol.ClientHello,
    Direction: protocol.ClientToServer,
    Name:      "ClientHello",
    Version:   1,
})
```

### Sending Messages

```go
// Create a signal
hello := client.NewHello("MyClient", "default", "user", "password")

// Encode to message
message, err := codec.EncodeMessage(hello)
if err != nil {
    return err
}

// Write to connection
err = codec.WriteMessage(conn, message)
```

### Receiving Messages

```go
// Read message from connection
message, err := codec.ReadMessage(conn)
if err != nil {
    return err
}

// Unpack into signal
signal, err := codec.UnpackSignal(message)
if err != nil {
    return err
}

// Handle based on type
switch signal.Type() {
case protocol.ClientHello:
    if hello, ok := signal.(*client.Hello); ok {
        // Handle client hello
    }
}
```

## Protocol Flow

### Client -> Server
1. Client creates signal (e.g., `ClientHello`)
2. Client calls `signal.Pack()` to serialize
3. Server receives message and calls `codec.UnpackSignal()`
4. Server gets signal instance and calls `signal.Unpack()`

### Server -> Client
1. Server creates signal (e.g., `ServerHello`)
2. Server calls `signal.Pack()` to serialize
3. Client receives message and calls `codec.UnpackSignal()`
4. Client gets signal instance and calls `signal.Unpack()`

## Key Benefits

1. **Single Source of Truth**: All protocol constants defined in one place
2. **Type Safety**: Strong typing for all signal types
3. **Automatic Direction Detection**: Codec automatically determines signal direction
4. **Extensible**: Easy to add new signal types
5. **Consistent**: Same serialization/deserialization logic everywhere
6. **Testable**: Clear interfaces make testing easier

## Migration Path

1. **Phase 1**: Create new protocol package alongside existing code
2. **Phase 2**: Gradually migrate existing protocol handlers to use new system
3. **Phase 3**: Remove old protocol implementations
4. **Phase 4**: Update SDK to use new protocol package

## Testing

The package includes comprehensive testing utilities:
- Registry testing with signal registration/retrieval
- Codec testing with message encoding/decoding
- Signal testing with pack/unpack operations
- Factory testing with signal instance creation

## Future Enhancements

1. **Protocol Versioning**: Support for multiple protocol versions
2. **Compression**: Built-in compression for large messages
3. **Encryption**: Optional message encryption
4. **Metrics**: Built-in protocol metrics and monitoring
5. **Validation**: Message validation and schema enforcement
