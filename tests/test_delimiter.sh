#!/bin/bash

# Test script to verify delimiter behavior
# This simulates what GeeseFS does when mounting

ENDPOINT="${ENDPOINT:-http://localhost:8080}"
BUCKET="testsync"
ACCESS_KEY="testtest"
SECRET_KEY="testtest"

echo "=== Test 1: List without delimiter (what GeeseFS does initially) ==="
aws s3api list-objects-v2 \
  --endpoint-url "$ENDPOINT" \
  --bucket "$BUCKET" \
  --prefix "" \
  --no-sign-request \
  --output json | jq -r '.Contents[]?.Key' | head -20

echo ""
echo "=== Test 2: List with delimiter=/ (hierarchical) ==="
aws s3api list-objects-v2 \
  --endpoint-url "$ENDPOINT" \
  --bucket "$BUCKET" \
  --prefix "" \
  --delimiter "/" \
  --no-sign-request \
  --output json | jq '{Contents: .Contents[]?.Key, CommonPrefixes: .CommonPrefixes[]?.Prefix}'

echo ""
echo "=== Test 3: Check what GeeseFS actually sends ==="
echo "Run: tcpdump -i any -A 'tcp port 8080' to see the actual HTTP requests"
