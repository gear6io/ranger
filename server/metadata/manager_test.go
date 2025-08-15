package metadata

import (
	"testing"
)

func TestMetadataManagerInterface(t *testing.T) {
	// This test verifies that MetadataManager implements MetadataManagerInterface
	var _ MetadataManagerInterface = (*MetadataManager)(nil)
}
