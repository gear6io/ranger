package native

// Client packet types
const (
	ClientHello    byte = 0
	ClientQuery    byte = 1
	ClientData     byte = 2
	ClientCancel   byte = 3
	ClientPing     byte = 4
	ClientAddendum byte = 110 // Addendum packet type for quota key and other extensions
)

// Server packet types
const (
	ServerHello                byte = 0
	ServerData                 byte = 1
	ServerException            byte = 2
	ServerProgress             byte = 3
	ServerPong                 byte = 4
	ServerEndOfStream          byte = 5
	ServerProfileInfo          byte = 6
	ServerTotals               byte = 7
	ServerExtremes             byte = 8
	ServerTablesStatusResponse byte = 9
	ServerLog                  byte = 10
	ServerTableColumns         byte = 11
	ServerPartUUIDs            byte = 12
	ServerReadTaskRequest      byte = 13
	ServerDataBlock            byte = 14
)

// Compression flags
const (
	CompressionEnabled  uint64 = 1
	CompressionDisabled uint64 = 0
)

// Query stages
const (
	QueryStageComplete           uint64 = 2
	QueryStageFetchColumns       uint64 = 1
	QueryStageWithMergeableState uint64 = 0
)
