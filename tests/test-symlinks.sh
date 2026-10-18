#!/bin/bash
set -e

echo "=== Symlink Test Suite ==="
echo ""

# Mount GeeseFS
echo "Step 1: Mounting GeeseFS..."
geesefs \
    --debug_s3 \
    --debug_fuse \
    --log-file=/data/log.txt \
    --file-mode=0666 \
    --dir-mode=0777 \
    --list-type=ext-v1 \
    --endpoint=http://s3syncer:8080/ \
    testsync \
    /mnt/testsync &

GEESEFS_PID=$!
sleep 3

if ! mountpoint -q /mnt/testsync; then
    echo "✗ Failed to mount GeeseFS"
    cat /data/log.txt
    exit 1
fi
echo "✓ GeeseFS mounted successfully"
echo ""

# Clean up any existing test files
echo "Step 1.5: Cleaning up old test files..."
rm -f /mnt/testsync/target.txt /mnt/testsync/link.txt 2>/dev/null || true
sleep 1
echo "✓ Cleanup complete"
echo ""

# Test 1: Create file and symlink
echo "Step 2: Creating test file..."
echo "Hello from target file" > /mnt/testsync/target.txt
if [ ! -f /mnt/testsync/target.txt ]; then
    echo "✗ Failed to create target file"
    exit 1
fi
echo "✓ Target file created"
echo ""

echo "Step 3: Creating symlink..."
ln -s target.txt /mnt/testsync/link.txt
if [ ! -L /mnt/testsync/link.txt ]; then
    echo "✗ Failed to create symlink"
    exit 1
fi
echo "✓ Symlink created"
echo ""

echo "Step 4: Verifying symlink target..."
LINK_TARGET=$(readlink /mnt/testsync/link.txt)
if [ "$LINK_TARGET" != "target.txt" ]; then
    echo "✗ Symlink target incorrect: expected 'target.txt', got '$LINK_TARGET'"
    exit 1
fi
echo "✓ Symlink target correct: $LINK_TARGET"
echo ""

echo "Step 5: Reading through symlink (before remount)..."
CONTENT=$(cat /mnt/testsync/link.txt)
EXPECTED="Hello from target file"
if [ "$CONTENT" != "$EXPECTED" ]; then
    echo "✗ Content mismatch"
    echo "  Expected: $EXPECTED"
    echo "  Got: $CONTENT"
    exit 1
fi
echo "✓ Content correct: $CONTENT"
echo ""

echo "Step 6: Listing directory..."
ls -la /mnt/testsync/
echo ""

# Unmount
echo "Step 7: Unmounting GeeseFS..."
fusermount -u /mnt/testsync || umount /mnt/testsync
wait $GEESEFS_PID 2>/dev/null || true
sleep 2

if mountpoint -q /mnt/testsync; then
    echo "✗ Failed to unmount"
    exit 1
fi
echo "✓ Unmounted successfully"
echo ""

# Remount
echo "Step 8: Remounting GeeseFS..."
geesefs \
    --debug_s3 \
    --debug_fuse \
    --log-file=/data/log.txt \
    --file-mode=0666 \
    --dir-mode=0777 \
    --list-type=ext-v1 \
    --endpoint=http://s3syncer:8080/ \
    testsync \
    /mnt/testsync &

GEESEFS_PID=$!
sleep 3

if ! mountpoint -q /mnt/testsync; then
    echo "✗ Failed to remount GeeseFS"
    cat /data/log.txt
    exit 1
fi
echo "✓ GeeseFS remounted successfully"
echo ""

echo "Step 9: Verifying files exist after remount..."
if [ ! -f /mnt/testsync/target.txt ]; then
    echo "✗ Target file not found after remount"
    ls -la /mnt/testsync/
    exit 1
fi
if [ ! -L /mnt/testsync/link.txt ]; then
    echo "✗ Symlink not found after remount"
    ls -la /mnt/testsync/
    exit 1
fi
echo "✓ Files exist after remount"
echo ""

echo "Step 10: Verifying symlink target after remount..."
LINK_TARGET=$(readlink /mnt/testsync/link.txt)
if [ "$LINK_TARGET" != "target.txt" ]; then
    echo "✗ Symlink target incorrect after remount: expected 'target.txt', got '$LINK_TARGET'"
    exit 1
fi
echo "✓ Symlink target correct after remount: $LINK_TARGET"
echo ""

echo "Step 11: Reading through symlink (after remount)..."
CONTENT=$(cat /mnt/testsync/link.txt)
if [ "$CONTENT" != "$EXPECTED" ]; then
    echo "✗ Content mismatch after remount"
    echo "  Expected: $EXPECTED"
    echo "  Got: $CONTENT"
    exit 1
fi
echo "✓ Content correct after remount: $CONTENT"
echo ""

echo "Step 12: Listing directory after remount..."
ls -la /mnt/testsync/
echo ""

# Cleanup
echo "Step 13: Cleaning up..."
fusermount -u /mnt/testsync || umount /mnt/testsync
wait $GEESEFS_PID 2>/dev/null || true
echo "✓ Cleanup complete"
echo ""

echo "=== All Symlink Tests Passed! ==="
