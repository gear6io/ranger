package config

// Network server port constants
// These ports are carefully selected to avoid conflicts with popular databases,
// data warehouses, and development tools commonly used in production environments.
const (
	// HTTP Server Port - REST API and web interface
	// Selected to avoid common development ports like 8080, 3000, 5000
	HTTP_SERVER_PORT = 2847

	// JDBC Server Port - PostgreSQL wire protocol compatible
	// Selected to avoid PostgreSQL (5432), MySQL (3306), SQL Server (1433), Oracle (1521)
	JDBC_SERVER_PORT = 2848

	// Native Protocol Server Port - Icebox-specific binary protocol
	// Selected to avoid ClickHouse (9000), MinIO (9000), Hadoop (50070+), Kafka (9092)
	NATIVE_SERVER_PORT = 2849

	// MinIO Server Port - Object storage service
	// Selected to avoid MinIO default (9000), S3-compatible services
	MINIO_SERVER_PORT = 2850

	// Health Check Port - Dedicated health monitoring endpoint
	// Selected to avoid common monitoring ports like 8080, 9090, 9100
	HEALTH_CHECK_PORT = 2851
)

// Network server address constants
const (
	// Default bind address for all servers
	DEFAULT_SERVER_ADDRESS = "0.0.0.0"

	// Localhost address for development
	LOCALHOST_ADDRESS = "127.0.0.1"
)

// Server enabled state constants
const (
	// All servers are enabled by default in production
	HTTP_SERVER_ENABLED   = true
	JDBC_SERVER_ENABLED   = true
	NATIVE_SERVER_ENABLED = true
)

// Port validation constants
const (
	MIN_PORT = 1
	MAX_PORT = 65535
)

// IsValidPort checks if a port number is within valid range
func IsValidPort(port int) bool {
	return port >= MIN_PORT && port <= MAX_PORT
}

// GetDefaultPorts returns a map of all default server ports
func GetDefaultPorts() map[string]int {
	return map[string]int{
		"http":   HTTP_SERVER_PORT,
		"jdbc":   JDBC_SERVER_PORT,
		"native": NATIVE_SERVER_PORT,
		"minio":  MINIO_SERVER_PORT,
		"health": HEALTH_CHECK_PORT,
	}
}

// GetDefaultAddresses returns a map of all default server addresses
func GetDefaultAddresses() map[string]string {
	return map[string]string{
		"http":   DEFAULT_SERVER_ADDRESS,
		"jdbc":   DEFAULT_SERVER_ADDRESS,
		"native": DEFAULT_SERVER_ADDRESS,
	}
}

// GetDefaultEnabledStates returns a map of all default server enabled states
func GetDefaultEnabledStates() map[string]bool {
	return map[string]bool{
		"http":   HTTP_SERVER_ENABLED,
		"jdbc":   JDBC_SERVER_ENABLED,
		"native": NATIVE_SERVER_ENABLED,
	}
}
