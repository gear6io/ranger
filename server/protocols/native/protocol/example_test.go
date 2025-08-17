package protocol

import (
	"bytes"
	"fmt"
	"log"
)

// Example demonstrates how to use the unified protocol system
func Example() {
	// 1. Create registry and factory
	registry := NewRegistry()
	factory := NewSignalFactory()

	// 2. Create codec
	codec := NewDefaultCodec(registry, factory)

	// 3. Register signal constructors
	factory.RegisterConstructor(ClientHello, func() Signal {
		return &MockSignal{signalType: ClientHello, data: []byte{}}
	})

	factory.RegisterConstructor(ServerHello, func() Signal {
		return &MockSignal{signalType: ServerHello, data: []byte{}}
	})

	// 4. Register signals with metadata
	clientHello := &MockSignal{signalType: ClientHello, data: []byte("client_hello_data")}
	serverHello := &MockSignal{signalType: ServerHello, data: []byte("server_hello_data")}

	registry.RegisterClientSignal(clientHello, &SignalInfo{
		Type:      ClientHello,
		Direction: ClientToServer,
		Name:      "ClientHello",
		Version:   1,
	})

	registry.RegisterServerSignal(serverHello, &SignalInfo{
		Type:      ServerHello,
		Direction: ServerToClient,
		Name:      "ServerHello",
		Version:   1,
	})

	// 5. Example: Client sends hello to server
	fmt.Println("=== Client -> Server ===")

	// Client creates and packs signal
	clientSignal := &MockSignal{signalType: ClientHello, data: []byte("Hello from client!")}
	message, err := codec.EncodeMessage(clientSignal)
	if err != nil {
		log.Fatalf("Failed to encode message: %v", err)
	}

	fmt.Printf("Client sent: Type=%d, Length=%d, Payload=%s\n",
		message.Type, message.Length, string(message.Payload))

	// Server receives and unpacks signal
	// Simulate receiving the message by writing to a buffer
	var buf bytes.Buffer
	err = codec.WriteMessage(&buf, message)
	if err != nil {
		log.Fatalf("Failed to write message: %v", err)
	}

	// Server reads the message
	receivedMessage, err := codec.ReadMessage(&buf)
	if err != nil {
		log.Fatalf("Failed to read message: %v", err)
	}

	// Server unpacks the signal
	receivedSignal, err := codec.UnpackSignal(receivedMessage)
	if err != nil {
		log.Fatalf("Failed to unpack signal: %v", err)
	}

	fmt.Printf("Server received: Type=%d, Data=%s\n",
		receivedSignal.Type(), string(receivedSignal.(*MockSignal).data))

	// 6. Example: Server sends hello to client
	fmt.Println("\n=== Server -> Client ===")

	// Server creates and packs signal
	serverSignal := &MockSignal{signalType: ServerHello, data: []byte("Hello from server!")}
	serverMessage, err := codec.EncodeMessage(serverSignal)
	if err != nil {
		log.Fatalf("Failed to encode server message: %v", err)
	}

	fmt.Printf("Server sent: Type=%d, Length=%d, Payload=%s\n",
		serverMessage.Type, serverMessage.Length, string(serverMessage.Payload))

	// Client receives and unpacks signal
	// Simulate receiving the message
	var clientBuf bytes.Buffer
	err = codec.WriteMessage(&clientBuf, serverMessage)
	if err != nil {
		log.Fatalf("Failed to write server message: %v", err)
	}

	// Client reads the message
	clientReceivedMessage, err := codec.ReadMessage(&clientBuf)
	if err != nil {
		log.Fatalf("Failed to read server message: %v", err)
	}

	// Client unpacks the signal
	clientReceivedSignal, err := codec.UnpackSignal(clientReceivedMessage)
	if err != nil {
		log.Fatalf("Failed to unpack server signal: %v", err)
	}

	fmt.Printf("Client received: Type=%d, Data=%s\n",
		clientReceivedSignal.Type(), string(clientReceivedSignal.(*MockSignal).data))

	// 7. Show registry information
	fmt.Println("\n=== Registry Information ===")
	fmt.Printf("Registered client signals: %v\n", registry.ListClientSignals())
	fmt.Printf("Registered server signals: %v\n", registry.ListServerSignals())

	// 8. Show signal info
	if info, err := registry.GetSignalInfo(ClientHello); err == nil {
		fmt.Printf("ClientHello info: %s (Direction: %v, Version: %d)\n",
			info.Name, info.Direction, info.Version)
	}

	if info, err := registry.GetSignalInfo(ServerHello); err == nil {
		fmt.Printf("ServerHello info: %s (Direction: %v, Version: %d)\n",
			info.Name, info.Direction, info.Version)
	}
}

// Note: MockSignal is already defined in registry_test.go
