package protocol

// Protocol constants matching ClickHouse native protocol
// See https://github.com/ClickHouse/ClickHouse/blob/master/src/Core/Protocol.h
const (
	// Client message types (Client -> Server)
	ClientHello  SignalType = 0
	ClientQuery  SignalType = 1
	ClientData   SignalType = 2
	ClientCancel SignalType = 3
	ClientPing   SignalType = 4

	// Server message types (Server -> Client)
	ServerHello               SignalType = 100
	ServerData                SignalType = 101
	ServerException           SignalType = 102
	ServerProgress            SignalType = 103
	ServerPong                SignalType = 104
	ServerEndOfStream         SignalType = 105
	ServerProfileInfo         SignalType = 106
	ServerTotals              SignalType = 107
	ServerExtremes            SignalType = 108
	ServerTablesStatus        SignalType = 109
	ServerLog                 SignalType = 110
	ServerTableColumns        SignalType = 111
	ServerPartUUIDs           SignalType = 112
	ServerReadTaskRequest     SignalType = 113
	ServerProfileEvents       SignalType = 114
	ServerTreeReadTaskRequest SignalType = 115
	ServerClose               SignalType = 116

	// Protocol versions
	DBMS_TCP_PROTOCOL_VERSION = 54460
)

// Signal type names for debugging and logging
var SignalTypeNames = map[SignalType]string{
	ClientHello:               "ClientHello",
	ClientQuery:               "ClientQuery",
	ClientData:                "ClientData",
	ClientCancel:              "ClientCancel",
	ClientPing:                "ClientPing",
	ServerHello:               "ServerHello",
	ServerData:                "ServerData",
	ServerException:           "ServerException",
	ServerProgress:            "ServerProgress",
	ServerPong:                "ServerPong",
	ServerEndOfStream:         "ServerEndOfStream",
	ServerProfileInfo:         "ServerProfileInfo",
	ServerTotals:              "ServerTotals",
	ServerExtremes:            "ServerExtremes",
	ServerTablesStatus:        "ServerTablesStatus",
	ServerLog:                 "ServerLog",
	ServerTableColumns:        "ServerTableColumns",
	ServerPartUUIDs:           "ServerPartUUIDs",
	ServerReadTaskRequest:     "ServerReadTaskRequest",
	ServerProfileEvents:       "ServerProfileEvents",
	ServerTreeReadTaskRequest: "ServerTreeReadTaskRequest",
	ServerClose:               "ServerClose",
}

// GetSignalName returns the human-readable name for a signal type
func GetSignalName(signalType SignalType) string {
	if name, exists := SignalTypeNames[signalType]; exists {
		return name
	}
	return "Unknown"
}

// IsClientSignal returns true if the signal type is sent from client to server
func IsClientSignal(signalType SignalType) bool {
	return signalType <= ClientPing
}

// IsServerSignal returns true if the signal type is sent from server to client
func IsServerSignal(signalType SignalType) bool {
	return signalType >= ServerHello
}
