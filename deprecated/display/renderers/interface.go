package renderers

import (
	"github.com/TFMV/icebox/deprecated/display"
)

// Renderer interface that all display renderers must implement
type Renderer interface {
	// Table rendering
	RenderTable(data display.TableData, options display.TableOptions) error

	// Message rendering
	RenderMessage(level display.MessageLevel, message string)

	// Interactive rendering
	RenderConfirm(message string) bool
	RenderSelect(message string, options []string) (int, error)
	RenderInput(message string) (string, error)

	// Progress rendering
	RenderProgress(title string, current, total int) error
}
