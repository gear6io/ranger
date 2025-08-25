package types

// QueryContext holds the context information for query execution
type QueryContext struct {
	Database   string
	User       string
	ClientAddr string
}
