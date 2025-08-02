#!/bin/bash

# Test script for clickhouse-client compatibility with icebox server
# This script runs all the mock queries supported by our icebox server
# and pipes the output to a temporary file for analysis

# Configuration
HOST="localhost"
PORT="9000"
USER="default"
PASSWORD=""
DATABASE="default"
OUTPUT_FILE="/tmp/icebox_clickhouse_compatibility_test_$(date +%Y%m%d_%H%M%S).log"

echo "=== Icebox ClickHouse Compatibility Test ===" | tee "$OUTPUT_FILE"
echo "Date: $(date)" | tee -a "$OUTPUT_FILE"
echo "Host: $HOST:$PORT" | tee -a "$OUTPUT_FILE"
echo "User: $USER" | tee -a "$OUTPUT_FILE"
echo "Database: $DATABASE" | tee -a "$OUTPUT_FILE"
echo "Output file: $OUTPUT_FILE" | tee -a "$OUTPUT_FILE"
echo "" | tee -a "$OUTPUT_FILE"

# Check if icebox server is running
echo "Checking if icebox server is running..." | tee -a "$OUTPUT_FILE"
if ! pgrep -f "icebox-server" > /dev/null; then
    echo "❌ Icebox server is not running. Please start it first with: make run-server" | tee -a "$OUTPUT_FILE"
    exit 1
fi
echo "✅ Icebox server is running" | tee -a "$OUTPUT_FILE"
echo "" | tee -a "$OUTPUT_FILE"

# Test queries based on the mock responses in server/protocols/native/connection.go
declare -a queries=(
    "SELECT 1"
    "SELECT 42"
    "SELECT 'hello'"
    "SELECT now()"
    "SELECT count(*)"
    "SELECT * FROM users"
    "SELECT * FROM orders"
    "SELECT * FROM unknown_table"
)

# Additional test queries for compatibility
declare -a additional_queries=(
    "SELECT 1 as test_column"
    "SELECT 1, 2, 3"
    "SELECT 'test' as string_column, 123 as int_column"
    "SELECT current_database()"
    "SELECT version()"
    "SHOW DATABASES"
    "SHOW TABLES"
    "DESCRIBE users"
    "DESCRIBE orders"
)

# Combine all queries
all_queries=("${queries[@]}" "${additional_queries[@]}")

echo "=== Testing ${#all_queries[@]} queries ===" | tee -a "$OUTPUT_FILE"
echo "" | tee -a "$OUTPUT_FILE"

# Function to test a single query
test_query() {
    local query="$1"
    local query_num="$2"
    local total_queries="$3"
    
    echo "--- Test $query_num/$total_queries: $query ---" | tee -a "$OUTPUT_FILE"
    
    # Run the query with clickhouse client
    local start_time=$(date +%s.%N)
    local output
    local exit_code
    
    output=$(clickhouse client \
        --host="$HOST" \
        --port="$PORT" \
        --user="$USER" \
        --database="$DATABASE" \
        --query="$query" \
        --format="PrettyCompact" \
        2>&1)
    exit_code=$?
    local end_time=$(date +%s.%N)
    local duration=$(echo "$end_time - $start_time" | bc -l)
    
    echo "Exit code: $exit_code" | tee -a "$OUTPUT_FILE"
    echo "Duration: ${duration}s" | tee -a "$OUTPUT_FILE"
    echo "Output:" | tee -a "$OUTPUT_FILE"
    echo "$output" | tee -a "$OUTPUT_FILE"
    
    if [ $exit_code -eq 0 ]; then
        echo "✅ SUCCESS" | tee -a "$OUTPUT_FILE"
    else
        echo "❌ FAILED" | tee -a "$OUTPUT_FILE"
    fi
    echo "" | tee -a "$OUTPUT_FILE"
}

# Test each query
for i in "${!all_queries[@]}"; do
    test_query "${all_queries[$i]}" $((i+1)) "${#all_queries[@]}"
done

# Summary
echo "=== Test Summary ===" | tee -a "$OUTPUT_FILE"
echo "Total queries tested: ${#all_queries[@]}" | tee -a "$OUTPUT_FILE"
echo "Output saved to: $OUTPUT_FILE" | tee -a "$OUTPUT_FILE"
echo "" | tee -a "$OUTPUT_FILE"

# Test connection and basic functionality
echo "=== Connection Test ===" | tee -a "$OUTPUT_FILE"
echo "Testing basic connection..." | tee -a "$OUTPUT_FILE"

connection_test=$(clickhouse client \
    --host="$HOST" \
    --port="$PORT" \
    --user="$USER" \
    --database="$DATABASE" \
    --query="SELECT 1 as connection_test" \
    --format="PrettyCompact" \
    2>&1)

if [ $? -eq 0 ]; then
    echo "✅ Connection successful" | tee -a "$OUTPUT_FILE"
    echo "Connection test output:" | tee -a "$OUTPUT_FILE"
    echo "$connection_test" | tee -a "$OUTPUT_FILE"
else
    echo "❌ Connection failed" | tee -a "$OUTPUT_FILE"
    echo "Connection test output:" | tee -a "$OUTPUT_FILE"
    echo "$connection_test" | tee -a "$OUTPUT_FILE"
fi

echo "" | tee -a "$OUTPUT_FILE"
echo "=== Test Complete ===" | tee -a "$OUTPUT_FILE"
echo "Check the output file for detailed results: $OUTPUT_FILE" | tee -a "$OUTPUT_FILE" 