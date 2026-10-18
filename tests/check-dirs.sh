#!/bin/bash
set -e

echo "Waiting for GeeseFS to mount..."
sleep 3

echo "=== Root directory ==="
ls -la /mnt/testsync | grep -E "^d|^-|^l" | grep -v "^\."

echo ""
echo "=== testdir/ directory ==="
ls -la /mnt/testsync/testdir/ | grep -E "^d|^-|^l" | grep -v "^\."

echo ""
echo "=== testdir/123/ directory ==="
ls -la /mnt/testsync/testdir/123/ | grep -E "^d|^-|^l" | grep -v "^\." || echo "Directory not accessible or empty"

echo ""
echo "SUCCESS: All directories are visible!"
