# Port Configuration

## Overview

Ranger now uses a **completely fixed and non-configurable** network server configuration. All network servers have predetermined ports, addresses, and enabled states that cannot be modified through configuration files.

## What's Fixed

### 1. Network Server Ports
- **HTTP Server**: Port 2847 (REST API and web interface)
- **JDBC Server**: Port 2848 (PostgreSQL wire protocol compatible)
- **Native Server**: Port 2849 (Ranger-specific binary protocol)
- **MinIO Server**: Port 2850 (Object storage service)
- **Health Check**: Port 2851 (Health monitoring endpoint)

### 2. Network Server Addresses
- **All Servers**: Bind to `0.0.0.0` (all interfaces)

### 3. Network Server States
- **All Servers**: Enabled by default

## Configuration Changes

### Before (Configurable)
```yaml
http:
  enabled: true
  address: "0.0.0.0"
  port: 8080

jdbc:
  enabled: false
  address: "127.0.0.1"
  port: 5432

native:
  enabled: true
  address: "0.0.0.0"
  port: 9000
```

### After (Fixed)
```yaml
# HTTP Server Configuration
# All settings are now fixed and non-configurable
# Port: 2847, Address: 0.0.0.0, Enabled: true

# JDBC Server Configuration
# All settings are now fixed and non-configurable
# Port: 2848, Address: 0.0.0.0, Enabled: true

# Native Protocol Server Configuration
# All settings are now fixed and non-configurable
# Port: 2849, Address: 0.0.0.0, Enabled: true
```

## Benefits

1. **No Configuration Conflicts**: Eliminates port conflicts between different environments
2. **Simplified Deployment**: No need to configure network settings per environment
3. **Consistent Behavior**: All deployments behave identically
4. **Reduced Errors**: No configuration-related network issues
5. **Easier Maintenance**: Single source of truth for network configuration

## Migration Guide

### 1. Remove Configuration Fields
Remove these fields from your configuration files:
- `http.enabled`
- `http.address`
- `http.port`
- `jdbc.enabled`
- `jdbc.address`
- `jdbc.port`
- `native.enabled`
- `native.address`
- `native.port`

### 2. Update Client Connections
Update client applications to use the new fixed ports:
- **HTTP/REST**: `localhost:2847`
- **JDBC**: `localhost:2848`
- **Native**: `localhost:2849`

### 3. Update Documentation
Update any documentation that references the old configurable ports.

## Code Changes

### Configuration Structs
```go
// Before
type HTTPConfig struct {
    Enabled bool   `yaml:"enabled"`
    Address string `yaml:"address"`
    Port    int    `yaml:"port"`
}

// After
type HTTPConfig struct {
    // All fields are now fixed and non-configurable
}
```

### Constants
```go
// Fixed ports
const (
    HTTP_SERVER_PORT   = 2847
    JDBC_SERVER_PORT   = 2848
    NATIVE_SERVER_PORT = 2849
)

// Fixed addresses
const (
    DEFAULT_SERVER_ADDRESS = "0.0.0.0"
)

// Fixed enabled states
const (
    HTTP_SERVER_ENABLED   = true
    JDBC_SERVER_ENABLED   = true
    NATIVE_SERVER_ENABLED = true
)
```

### Server Implementations
```go
// Before
if !s.config.Enabled {
    return nil
}
port := s.config.Port
addr := fmt.Sprintf("%s:%d", s.config.Address, port)

// After
if !config.HTTP_SERVER_ENABLED {
    return nil
}
port := config.HTTP_SERVER_PORT
addr := fmt.Sprintf("%s:%d", config.DEFAULT_SERVER_ADDRESS, port)
```

## Future Considerations

1. **Environment Overrides**: If needed, environment variables could be added for development/testing
2. **Port Ranges**: Could implement port range validation for custom deployments
3. **Service Discovery**: Could integrate with service discovery systems
4. **Load Balancing**: Could add load balancer configuration

## Troubleshooting

### Port Already in Use
If you get "port already in use" errors:
1. Check if another service is using the fixed ports
2. Stop conflicting services
3. Use `netstat -an | grep 2847` to check port usage

### Connection Refused
If clients can't connect:
1. Verify the server is running
2. Check firewall settings
3. Confirm you're using the correct fixed ports

### Configuration Warnings
If you see warnings about missing configuration fields:
1. Remove the old configurable fields from your YAML files
2. The server will use the fixed values automatically

## Summary

Ranger now provides a **zero-configuration** network server setup with carefully selected, non-conflicting ports. This approach eliminates configuration complexity while ensuring consistent behavior across all deployments.
