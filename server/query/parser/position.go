// Package parser - Position tracking for enhanced error reporting
// Copyright (C) Alex Gaetano Padula
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.
package parser

import (
	"sort"
	"unicode/utf8"
)

// TokenPosition stores precise location information for a token
// Uses efficient field layout with lazy line/column calculation
type TokenPosition struct {
	Offset int // Byte offset in source (primary position, always available)
	Length int // Token length in bytes
	// Line and Column are calculated lazily when needed via PositionTracker
}

// PositionTracker efficiently tracks line/column positions with lazy evaluation
type PositionTracker struct {
	source     []byte // Original source code (reference, not copy)
	lineStarts []int  // Byte offsets where each line starts (calculated once, cached)
	calculated bool   // Whether line starts have been calculated (lazy initialization flag)
}

// NewPositionTracker creates a new position tracker for the given source
func NewPositionTracker(source []byte) *PositionTracker {
	return &PositionTracker{
		source:     source,
		calculated: false,
	}
}

// calculateLineStarts performs lazy initialization of line start positions
// Handles different line ending types (LF, CRLF, CR) efficiently
func (pt *PositionTracker) calculateLineStarts() {
	if pt.calculated {
		return
	}

	pt.lineStarts = []int{0} // First line starts at offset 0

	for i := 0; i < len(pt.source); i++ {
		switch pt.source[i] {
		case '\n': // LF (Unix)
			pt.lineStarts = append(pt.lineStarts, i+1)
		case '\r': // CR (Mac) or CRLF (Windows)
			if i+1 < len(pt.source) && pt.source[i+1] == '\n' {
				// CRLF - skip the LF, it will be handled by the LF case
				i++ // Skip the \n
				pt.lineStarts = append(pt.lineStarts, i+1)
			} else {
				// Just CR
				pt.lineStarts = append(pt.lineStarts, i+1)
			}
		}
	}

	pt.calculated = true
}

// GetLineColumn returns the 1-based line and column numbers for the given byte offset
// Uses binary search for performance with large files
func (pt *PositionTracker) GetLineColumn(offset int) (line, column int) {
	pt.calculateLineStarts()

	if offset < 0 {
		return 1, 1
	}
	if offset >= len(pt.source) {
		// Handle end of file
		if len(pt.lineStarts) == 0 {
			return 1, 1
		}
		lastLineStart := pt.lineStarts[len(pt.lineStarts)-1]
		lastLine := len(pt.lineStarts)
		lastColumn := pt.calculateColumn(lastLineStart, len(pt.source))
		return lastLine, lastColumn
	}

	// Binary search to find the line containing this offset
	lineIndex := sort.Search(len(pt.lineStarts), func(i int) bool {
		return pt.lineStarts[i] > offset
	}) - 1

	if lineIndex < 0 {
		lineIndex = 0
	}

	lineStart := pt.lineStarts[lineIndex]
	line = lineIndex + 1 // 1-based line number
	column = pt.calculateColumn(lineStart, offset)

	return line, column
}

// calculateColumn calculates the 1-based column number with proper UTF-8 support
// Handles multi-byte characters correctly
func (pt *PositionTracker) calculateColumn(lineStart, offset int) int {
	if offset <= lineStart {
		return 1
	}

	lineBytes := pt.source[lineStart:offset]
	return utf8.RuneCount(lineBytes) + 1 // 1-based column
}

// GetSourceLine returns the source line at the given 1-based line number
// Used for error context display
func (pt *PositionTracker) GetSourceLine(line int) string {
	pt.calculateLineStarts()

	if line < 1 || line > len(pt.lineStarts) {
		return ""
	}

	lineIndex := line - 1 // Convert to 0-based
	lineStart := pt.lineStarts[lineIndex]

	var lineEnd int
	if lineIndex+1 < len(pt.lineStarts) {
		lineEnd = pt.lineStarts[lineIndex+1] - 1
		// Remove line ending characters
		for lineEnd > lineStart && (pt.source[lineEnd] == '\n' || pt.source[lineEnd] == '\r') {
			lineEnd--
		}
		lineEnd++ // Include the last character
	} else {
		lineEnd = len(pt.source)
	}

	if lineStart >= lineEnd {
		return ""
	}

	return string(pt.source[lineStart:lineEnd])
}

// GetLineCount returns the total number of lines in the source
func (pt *PositionTracker) GetLineCount() int {
	pt.calculateLineStarts()
	return len(pt.lineStarts)
}

// IsValidOffset checks if the given offset is valid for this source
func (pt *PositionTracker) IsValidOffset(offset int) bool {
	return offset >= 0 && offset <= len(pt.source)
}
