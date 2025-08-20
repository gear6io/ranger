package native

// Protocol constants matching ClickHouse native protocol
// See https://github.com/ClickHouse/ClickHouse/blob/master/src/Core/Protocol.h
const (
	// Client message types
	ClientHello  = 0
	ClientQuery  = 1
	ClientData   = 2
	ClientCancel = 3
	ClientPing   = 4

	// Server message types
	ServerHello               = 0
	ServerData                = 1
	ServerException           = 2
	ServerProgress            = 3
	ServerPong                = 4
	ServerEndOfStream         = 5
	ServerProfileInfo         = 6
	ServerTotals              = 7
	ServerExtremes            = 8
	ServerTablesStatus        = 9
	ServerLog                 = 10
	ServerTableColumns        = 11
	ServerPartUUIDs           = 12
	ServerReadTaskRequest     = 13
	ServerProfileEvents       = 14
	ServerTreeReadTaskRequest = 15

	// Protocol versions
	DBMS_TCP_PROTOCOL_VERSION = 54460
)

// Block info structure for data transfer
type BlockInfo struct {
	IsOverflows bool
	BucketNum   int32
}

// Data block structure
type DataBlock struct {
	TableName   string
	Columns     []string
	ColumnTypes []string
	Rows        [][]interface{}
}
