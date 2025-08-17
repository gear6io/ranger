package protocol

import (
	"fmt"
	"reflect"
)

// SignalFactory creates new instances of signals
type SignalFactory struct {
	// Map of signal type to constructor function
	constructors map[SignalType]func() Signal
}

// NewSignalFactory creates a new signal factory
func NewSignalFactory() *SignalFactory {
	return &SignalFactory{
		constructors: make(map[SignalType]func() Signal),
	}
}

// RegisterConstructor registers a constructor function for a signal type
func (f *SignalFactory) RegisterConstructor(signalType SignalType, constructor func() Signal) {
	f.constructors[signalType] = constructor
}

// CreateSignal creates a new signal instance of the specified type
func (f *SignalFactory) CreateSignal(signalType SignalType) (Signal, error) {
	constructor, exists := f.constructors[signalType]
	if !exists {
		return nil, fmt.Errorf("no constructor registered for signal type %d", signalType)
	}

	return constructor(), nil
}

// CreateSignalFromInstance creates a new signal instance from an existing signal
// This is useful when you have a signal instance and want to create a new one of the same type
func (f *SignalFactory) CreateSignalFromInstance(signal Signal) (Signal, error) {
	// Use reflection to create a new instance of the same type
	signalType := reflect.TypeOf(signal)
	if signalType.Kind() == reflect.Ptr {
		signalType = signalType.Elem()
	}

	// Create a new instance
	newInstance := reflect.New(signalType)

	// Convert back to Signal interface
	if newSignal, ok := newInstance.Interface().(Signal); ok {
		return newSignal, nil
	}

	return nil, fmt.Errorf("failed to create new instance of signal type %T", signal)
}

// IsRegistered returns true if a constructor is registered for the signal type
func (f *SignalFactory) IsRegistered(signalType SignalType) bool {
	_, exists := f.constructors[signalType]
	return exists
}

// ListRegisteredTypes returns all registered signal types
func (f *SignalFactory) ListRegisteredTypes() []SignalType {
	types := make([]SignalType, 0, len(f.constructors))
	for signalType := range f.constructors {
		types = append(types, signalType)
	}
	return types
}

// Clear removes all registered constructors (useful for testing)
func (f *SignalFactory) Clear() {
	f.constructors = make(map[SignalType]func() Signal)
}
