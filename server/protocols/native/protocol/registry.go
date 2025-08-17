package protocol

import (
	"fmt"
	"sync"
)

// Registry manages signal types and their implementations
type Registry struct {
	mu sync.RWMutex

	// Client signals (Client -> Server)
	clientSignals map[SignalType]Signal

	// Server signals (Server -> Client)
	serverSignals map[SignalType]Signal

	// Signal metadata
	signalInfo map[SignalType]*SignalInfo
}

// NewRegistry creates a new signal registry
func NewRegistry() *Registry {
	return &Registry{
		clientSignals: make(map[SignalType]Signal),
		serverSignals: make(map[SignalType]Signal),
		signalInfo:    make(map[SignalType]*SignalInfo),
	}
}

// RegisterClientSignal registers a client signal implementation
func (r *Registry) RegisterClientSignal(signal Signal, info *SignalInfo) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !IsClientSignal(signal.Type()) {
		return fmt.Errorf("signal type %d is not a client signal", signal.Type())
	}

	if _, exists := r.clientSignals[signal.Type()]; exists {
		return fmt.Errorf("client signal type %d already registered", signal.Type())
	}

	r.clientSignals[signal.Type()] = signal
	r.signalInfo[signal.Type()] = info

	return nil
}

// RegisterServerSignal registers a server signal implementation
func (r *Registry) RegisterServerSignal(signal Signal, info *SignalInfo) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !IsServerSignal(signal.Type()) {
		return fmt.Errorf("signal type %d is not a server signal", signal.Type())
	}

	if _, exists := r.serverSignals[signal.Type()]; exists {
		return fmt.Errorf("server signal type %d already registered", signal.Type())
	}

	r.serverSignals[signal.Type()] = signal
	r.signalInfo[signal.Type()] = info

	return nil
}

// GetClientSignal returns a client signal implementation by type
func (r *Registry) GetClientSignal(signalType SignalType) (Signal, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	signal, exists := r.clientSignals[signalType]
	if !exists {
		return nil, fmt.Errorf("client signal type %d not registered", signalType)
	}

	return signal, nil
}

// GetServerSignal returns a server signal implementation by type
func (r *Registry) GetServerSignal(signalType SignalType) (Signal, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	signal, exists := r.serverSignals[signalType]
	if !exists {
		return nil, fmt.Errorf("server signal type %d not registered", signalType)
	}

	return signal, nil
}

// GetSignalInfo returns metadata for a signal type
func (r *Registry) GetSignalInfo(signalType SignalType) (*SignalInfo, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	info, exists := r.signalInfo[signalType]
	if !exists {
		return nil, fmt.Errorf("signal type %d not registered", signalType)
	}

	return info, nil
}

// ListClientSignals returns all registered client signal types
func (r *Registry) ListClientSignals() []SignalType {
	r.mu.RLock()
	defer r.mu.RUnlock()

	types := make([]SignalType, 0, len(r.clientSignals))
	for signalType := range r.clientSignals {
		types = append(types, signalType)
	}

	return types
}

// ListServerSignals returns all registered server signal types
func (r *Registry) ListServerSignals() []SignalType {
	r.mu.RLock()
	defer r.mu.RUnlock()

	types := make([]SignalType, 0, len(r.serverSignals))
	for signalType := range r.serverSignals {
		types = append(types, signalType)
	}

	return types
}

// IsRegistered returns true if a signal type is registered
func (r *Registry) IsRegistered(signalType SignalType) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	_, clientExists := r.clientSignals[signalType]
	_, serverExists := r.serverSignals[signalType]

	return clientExists || serverExists
}

// Clear removes all registered signals (useful for testing)
func (r *Registry) Clear() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.clientSignals = make(map[SignalType]Signal)
	r.serverSignals = make(map[SignalType]Signal)
	r.signalInfo = make(map[SignalType]*SignalInfo)
}
