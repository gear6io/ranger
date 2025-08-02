package native

import (
	"fmt"
	"io"
	"net"
	"time"

	"github.com/rs/zerolog"
)

// ConnectionHandler handles a single client connection
type ConnectionHandler struct {
	conn      net.Conn
	logger    zerolog.Logger
	reader    *PacketReader
	writer    *PacketWriter
	helloSent bool
}

// NewConnectionHandler creates a new connection handler
func NewConnectionHandler(conn net.Conn, logger zerolog.Logger) *ConnectionHandler {
	return &ConnectionHandler{
		conn:      conn,
		logger:    logger,
		reader:    NewPacketReader(conn),
		writer:    NewPacketWriter(conn),
		helloSent: false,
	}
}

// Handle handles the client connection
func (h *ConnectionHandler) Handle() error {
	defer h.conn.Close()

	// Set connection timeout
	h.conn.SetDeadline(time.Now().Add(30 * time.Second))

	// Handle client packets
	for {
		// Reset deadline
		h.conn.SetDeadline(time.Now().Add(30 * time.Second))

		// Read packet type
		packetType, err := h.reader.ReadByte()
		if err != nil {
			if err == io.EOF {
				return nil // Client disconnected
			}
			return fmt.Errorf("failed to read packet type: %w", err)
		}

		// Handle packet based on type
		switch packetType {
		case ClientHello:
			if err := h.handleClientHello(); err != nil {
				return fmt.Errorf("failed to handle client hello: %w", err)
			}
			// Send server hello immediately after client hello
			if err := h.sendHello(); err != nil {
				return fmt.Errorf("failed to send hello: %w", err)
			}
			h.helloSent = true

			// After handshake, check for addendum (quota key)
			// This is sent as a string, not as a packet type
			if err := h.handleAddendum(); err != nil {
				return fmt.Errorf("failed to handle addendum: %w", err)
			}
		case ClientQuery:
			if err := h.handleQuery(); err != nil {
				return fmt.Errorf("failed to handle query: %w", err)
			}
		case ClientCancel:
			if err := h.handleCancel(); err != nil {
				return fmt.Errorf("failed to handle cancel: %w", err)
			}
		case ClientPing:
			if err := h.handlePing(); err != nil {
				return fmt.Errorf("failed to handle ping: %w", err)
			}
		case ClientData:
			if err := h.handleData(); err != nil {
				return fmt.Errorf("failed to handle data: %w", err)
			}
		default:
			// Handle unknown packet types - might be addendum or other protocol extensions
			h.logger.Debug().Uint8("packet_type", packetType).Msg("Unknown packet type received")
			if err := h.handleUnknownPacket(packetType); err != nil {
				return fmt.Errorf("failed to handle unknown packet %d: %w", packetType, err)
			}
		}
	}
}

// sendHello sends server hello packet
func (h *ConnectionHandler) sendHello() error {
	// Write packet type
	if err := h.writer.WriteByte(ServerHello); err != nil {
		return err
	}
	// Write server name
	if err := h.writer.WriteString("Icebox"); err != nil {
		return err
	}
	// Write major version
	if err := h.writer.WriteUvarint(22); err != nil {
		return err
	}
	// Write minor version
	if err := h.writer.WriteUvarint(3); err != nil {
		return err
	}
	// Write revision
	if err := h.writer.WriteUvarint(54460); err != nil {
		return err
	}
	// Write timezone
	if err := h.writer.WriteString("UTC"); err != nil {
		return err
	}
	// Write display name
	if err := h.writer.WriteString("Icebox"); err != nil {
		return err
	}
	// Write version patch
	if err := h.writer.WriteUvarint(1); err != nil {
		return err
	}
	return h.writer.Flush()
}

// handleClientHello handles client hello packet
func (h *ConnectionHandler) handleClientHello() error {
	// Read client name
	clientName, err := h.reader.ReadString()
	if err != nil {
		return err
	}

	// Read major version
	majorVersion, err := h.reader.ReadUvarint()
	if err != nil {
		return err
	}

	// Read minor version
	minorVersion, err := h.reader.ReadUvarint()
	if err != nil {
		return err
	}

	// Read protocol version (revision)
	protocolVersion, err := h.reader.ReadUvarint()
	if err != nil {
		return err
	}

	// Read default database
	database, err := h.reader.ReadString()
	if err != nil {
		return err
	}

	// Read username
	username, err := h.reader.ReadString()
	if err != nil {
		return err
	}

	// Read password
	password, err := h.reader.ReadString()
	if err != nil {
		return err
	}

	h.logger.Debug().
		Str("client", clientName).
		Uint64("major", majorVersion).
		Uint64("minor", minorVersion).
		Uint64("protocol_version", protocolVersion).
		Str("database", database).
		Str("username", username).
		Str("password", "***").
		Msg("Client hello received")

	// TODO: Validate password if authentication is implemented
	_ = password

	// No response needed - server hello will be sent by the main handler
	return nil
}

// handleQuery handles client query packet
func (h *ConnectionHandler) handleQuery() error {
	// Read query ID
	queryID, err := h.reader.ReadString()
	if err != nil {
		return err
	}

	// Read client info
	clientInfo, err := h.reader.ReadString()
	if err != nil {
		return err
	}

	// Read query
	query, err := h.reader.ReadString()
	if err != nil {
		return err
	}

	h.logger.Debug().
		Str("query_id", queryID).
		Str("client_info", clientInfo).
		Str("query", query).
		Msg("Query received")

	// TODO: Execute query using engine
	// For now, send a simple response
	return h.sendQueryResponse(query)
}

// handlePing handles client ping packet
func (h *ConnectionHandler) handlePing() error {
	h.logger.Debug().Msg("Ping received")
	return h.sendPong()
}

// handleData handles client data packet
func (h *ConnectionHandler) handleData() error {
	// Read table name
	tableName, err := h.reader.ReadString()
	if err != nil {
		return err
	}

	h.logger.Debug().Str("table", tableName).Msg("Data packet received")

	// TODO: Handle data insertion
	// For now, just acknowledge
	return h.sendDataResponse()
}

// handleCancel handles client cancel packet
func (h *ConnectionHandler) handleCancel() error {
	h.logger.Debug().Msg("Cancel received")
	// TODO: Cancel running query
	return nil
}

// handleAddendum handles the addendum (quota key) sent after handshake
func (h *ConnectionHandler) handleAddendum() error {
	// Try to read addendum as a string (quota key)
	// This is sent by ClickHouse Go client after handshake
	quotaKey, err := h.reader.ReadString()
	if err != nil {
		// If no addendum, that's fine
		return nil
	}
	h.logger.Debug().Str("quota_key", quotaKey).Msg("Addendum received")
	return nil
}

// handleUnknownPacket handles unknown packet types (like addendum)
func (h *ConnectionHandler) handleUnknownPacket(packetType byte) error {
	// Handle addendum (quota key) - this is sent as a string after handshake
	if packetType == 110 { // This might be the addendum packet type
		quotaKey, err := h.reader.ReadString()
		if err != nil {
			return err
		}
		h.logger.Debug().Str("quota_key", quotaKey).Msg("Addendum received")
		return nil
	}

	// For other unknown packets, just ignore them
	h.logger.Debug().Uint8("packet_type", packetType).Msg("Ignoring unknown packet type")
	return nil
}

// sendPong sends pong response
func (h *ConnectionHandler) sendPong() error {
	if err := h.writer.WriteByte(ServerPong); err != nil {
		return err
	}
	return h.writer.Flush()
}

// sendQueryResponse sends query response
func (h *ConnectionHandler) sendQueryResponse(query string) error {
	// Send data packet type
	if err := h.writer.WriteByte(ServerData); err != nil {
		return err
	}

	// Send column count
	if err := h.writer.WriteUvarint(1); err != nil {
		return err
	}

	// Send column name
	if err := h.writer.WriteString("result"); err != nil {
		return err
	}

	// Send column type
	if err := h.writer.WriteString("UInt8"); err != nil {
		return err
	}

	// Send data block
	if err := h.writer.WriteUvarint(1); err != nil {
		return err
	}

	// Send row count
	if err := h.writer.WriteUvarint(1); err != nil {
		return err
	}

	// Send data (UInt8 value 1)
	if err := h.writer.WriteByte(1); err != nil {
		return err
	}

	// Send end of stream
	if err := h.writer.WriteByte(ServerEndOfStream); err != nil {
		return err
	}

	return h.writer.Flush()
}

// sendDataResponse sends data response
func (h *ConnectionHandler) sendDataResponse() error {
	// Send progress
	if err := h.writer.WriteByte(ServerProgress); err != nil {
		return err
	}

	if err := h.writer.WriteUvarint(1); err != nil {
		return err
	}

	// Send end of stream
	if err := h.writer.WriteByte(ServerEndOfStream); err != nil {
		return err
	}

	return h.writer.Flush()
}
