#!/bin/bash
# Test script for task management system

set -e

CONFIG="config.yaml"
SERVER_PID=""

cleanup() {
    if [ -n "$SERVER_PID" ]; then
        echo "Stopping server (PID: $SERVER_PID)..."
        kill $SERVER_PID 2>/dev/null || true
        wait $SERVER_PID 2>/dev/null || true
    fi
}

trap cleanup EXIT

echo "=== Starting s3syncer server ==="
./bin/s3syncer -config $CONFIG &
SERVER_PID=$!
echo "Server started with PID: $SERVER_PID"

# Wait for server to start
sleep 2

echo ""
echo "=== Testing task-list (should be empty) ==="
./bin/s3syncer -config $CONFIG task-list

echo ""
echo "=== Triggering resync task ==="
./bin/s3syncer -config $CONFIG resync

echo ""
echo "=== Waiting 2 seconds ==="
sleep 2

echo ""
echo "=== Listing tasks ==="
./bin/s3syncer -config $CONFIG task-list

echo ""
echo "=== Test completed successfully ==="
