package display

import (
	"context"
	"log"
)

// contextKey is a type for context keys
type contextKey string

// displayContextKey is the key for storing display in context
const displayContextKey contextKey = "display"

// DisplayContext holds display-related context information
type DisplayContext struct {
	Display Display
	Logger  *log.Logger
	Verbose bool
	Format  OutputFormat
}

// WithDisplay adds a display instance to the context
func WithDisplay(ctx context.Context, display Display) context.Context {
	return context.WithValue(ctx, displayContextKey, &DisplayContext{
		Display: display,
		Logger:  log.Default(),
	})
}

// WithDisplayContext adds a full display context to the context
func WithDisplayContext(ctx context.Context, dc *DisplayContext) context.Context {
	return context.WithValue(ctx, displayContextKey, dc)
}

// GetDisplay retrieves the display instance from context
func GetDisplay(ctx context.Context) Display {
	if dc := GetDisplayContext(ctx); dc != nil {
		return dc.Display
	}
	return nil
}

// GetDisplayContext retrieves the full display context
func GetDisplayContext(ctx context.Context) *DisplayContext {
	if val := ctx.Value(displayContextKey); val != nil {
		if dc, ok := val.(*DisplayContext); ok {
			return dc
		}
	}
	return nil
}

// GetDisplayOrDefault retrieves the display instance from context or returns a default
func GetDisplayOrDefault(ctx context.Context) Display {
	if d := GetDisplay(ctx); d != nil {
		return d
	}
	return New()
}

// LogOrDisplay logs a message using the display if available, otherwise uses the logger
func LogOrDisplay(ctx context.Context, level MessageLevel, format string, args ...interface{}) {
	dc := GetDisplayContext(ctx)
	if dc != nil && dc.Display != nil {
		switch level {
		case MessageLevelSuccess:
			dc.Display.Success(format, args...)
		case MessageLevelError:
			dc.Display.Error(format, args...)
		case MessageLevelWarning:
			dc.Display.Warning(format, args...)
		case MessageLevelInfo:
			dc.Display.Info(format, args...)
		}
	} else if dc != nil && dc.Logger != nil {
		dc.Logger.Printf(format, args...)
	} else {
		log.Printf(format, args...)
	}
}
