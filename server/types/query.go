package types

// QueryContext holds the context information for query execution
type QueryContext struct {
	Query      string
	Database   string
	User       string
	ClientAddr string
}
