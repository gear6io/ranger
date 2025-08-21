package shared

import "context"

// Component defines the interface that all system components must implement
type Component interface {
	// GetType returns the component type identifier
	GetType() string

	// Shutdown gracefully shuts down the component
	Shutdown(ctx context.Context) error
}
