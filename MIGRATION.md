# Migration Guide: Monolithic to Client-Server Architecture

## Overview

This guide helps you migrate from the current monolithic icebox CLI to the new client-server architecture.

## What's Changed

### Before (Monolithic)
```bash
./icebox serve --port 8080
./icebox sql "SELECT * FROM table"
./icebox import data.parquet --table sales
```

### After (Client-Server)
```bash
# Terminal 1: Start server
./icebox-server

# Terminal 2: Use client
./icebox-client query "SELECT * FROM table"
./icebox-client import data.parquet --table sales
```

## Migration Steps

### 1. Build New Components

```bash
# Build both server and client
./scripts/build.sh

# Or build individually
./scripts/build-server.sh
./scripts/build-client.sh
```

### 2. Start Server

```bash
# Using default configuration
./bin/icebox-server

# Using custom configuration
./bin/icebox-server --config icebox-server.yml
```

### 3. Use Client

```bash
# Execute queries
./bin/icebox-client query "SELECT 1"

# Interactive shell
./bin/icebox-client shell

# Import data
./bin/icebox-client import data.parquet --table sales
```

### 4. Docker Deployment

```bash
# Start server and client
docker-compose up -d

# Use client
docker-compose exec icebox-client ./icebox-client shell
```

## Configuration Migration

### Old Configuration
The old `.icebox.yml` configuration is still supported for backward compatibility.

### New Configuration

#### Server Configuration (`icebox-server.yml`)
```yaml
version: "0.1.0"
http:
  enabled: true
  port: 8080
jdbc:
  enabled: true
  port: 5432
storage:
  type: "filesystem"
  filesystem:
    root_path: "./data"
```

#### Client Configuration (`icebox-client.yml`)
```yaml
server:
  address: "localhost:8080"
  timeout: 30s
database:
  name: "default"
```

## Backward Compatibility

The old monolithic CLI (`./icebox`) will continue to work during the transition period. You can:

1. Use the old CLI for existing workflows
2. Gradually migrate to the new client-server architecture
3. Run both simultaneously for testing

## Testing

### Test Server
```bash
# Start server
./bin/icebox-server

# Test HTTP API
curl http://localhost:8080/health

# Test JDBC (if you have a PostgreSQL client)
psql -h localhost -p 5432 -U test -d icebox
```

### Test Client
```bash
# Test basic functionality
./bin/icebox-client query "SELECT 1 as test"

# Test shell
./bin/icebox-client shell
```

## Troubleshooting

### Common Issues

1. **Port already in use**: Change ports in configuration
2. **Permission denied**: Ensure proper file permissions
3. **Connection refused**: Check if server is running

### Logs

- Server logs: Check console output or log files
- Client logs: Use `--verbose` flag for detailed output

## Next Steps

1. Test the new architecture thoroughly
2. Update deployment scripts
3. Update documentation
4. Plan production deployment
5. Deprecate old monolithic CLI (future release)
