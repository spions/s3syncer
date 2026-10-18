#!/bin/bash
set -e

echo "=== GeeseFS Directory Persistence Test (Simplified) ==="
echo ""

MOUNT_POINT="/mnt/testsync"
TEST_DIR="test"
TEST_SUBDIR1="test2"
TEST_SUBDIR2="test3"

echo "Step 1: Creating directory structure via GeeseFS..."
mkdir -p "$MOUNT_POINT/$TEST_DIR/$TEST_SUBDIR1/$TEST_SUBDIR2"
echo "test content" > "$MOUNT_POINT/$TEST_DIR/$TEST_SUBDIR1/$TEST_SUBDIR2/file.txt"

echo "Created structure:"
echo "  $TEST_DIR/"
echo "  $TEST_DIR/$TEST_SUBDIR1/"
echo "  $TEST_DIR/$TEST_SUBDIR1/$TEST_SUBDIR2/"
echo "  $TEST_DIR/$TEST_SUBDIR1/$TEST_SUBDIR2/file.txt"
sleep 2
echo ""

echo "Step 2: Verifying structure before remount..."
if [ -d "$MOUNT_POINT/$TEST_DIR/$TEST_SUBDIR1/$TEST_SUBDIR2" ]; then
    echo "✓ All directories visible before remount"
    ls -la "$MOUNT_POINT/$TEST_DIR/$TEST_SUBDIR1/" | grep -E "^d|^-"
else
    echo "✗ Directories not visible before remount"
    exit 1
fi
echo ""

echo "Step 3: Unmounting GeeseFS..."
fusermount -u "$MOUNT_POINT" || umount "$MOUNT_POINT" || true
sleep 2
echo ""

echo "Step 4: Remounting GeeseFS..."
geesefs \
    --log-file=/data/log-remount.txt \
    --file-mode=0666 \
    --dir-mode=0777 \
    --endpoint=http://s3syncer:8080/ \
    testsync \
    "$MOUNT_POINT" &

GEESEFS_PID=$!
sleep 3
echo ""

echo "Step 5: Checking directory structure after remount..."
echo "Root directory:"
ls -la "$MOUNT_POINT/" | grep -E "^d|^-" | grep -v "^\."
echo ""

echo "Checking $TEST_DIR/:"
if ls -la "$MOUNT_POINT/$TEST_DIR/" 2>/dev/null | grep -E "^d|^-" | grep -v "^\."; then
    echo "✓ $TEST_DIR/ is accessible"
else
    echo "✗ $TEST_DIR/ is NOT accessible"
fi
echo ""

echo "Checking $TEST_DIR/$TEST_SUBDIR1/:"
if ls -la "$MOUNT_POINT/$TEST_DIR/$TEST_SUBDIR1/" 2>/dev/null | grep -E "^d|^-" | grep -v "^\."; then
    echo "✓ $TEST_DIR/$TEST_SUBDIR1/ is accessible"

    echo ""
    echo "Checking $TEST_DIR/$TEST_SUBDIR1/$TEST_SUBDIR2/:"
    if ls -la "$MOUNT_POINT/$TEST_DIR/$TEST_SUBDIR1/$TEST_SUBDIR2/" 2>/dev/null | grep -E "^d|^-" | grep -v "^\."; then
        echo "✓✓✓ TEST PASSED: All directories persist after remount!"
        echo ""
        echo "This means directory markers were created correctly."
        exit 0
    else
        echo "✗ $TEST_DIR/$TEST_SUBDIR1/$TEST_SUBDIR2/ NOT visible after remount"
        NEED_FIX=1
    fi
else
    echo "✗ $TEST_DIR/$TEST_SUBDIR1/ NOT visible after remount"
    NEED_FIX=1
fi

if [ "$NEED_FIX" = "1" ]; then
    echo ""
    echo "=== DIAGNOSIS ==="
    echo "The problem: GeeseFS mkdir doesn't create directory marker objects in S3."
    echo "When you create a directory via mkdir, GeeseFS only creates it in memory."
    echo "After remount, GeeseFS can't see directories that don't have:"
    echo "  1. A directory marker object (empty object with key 'dirname/')"
    echo "  2. OR at least one file inside the directory"
    echo ""
    echo "Solution: Always create a file inside each directory, or use tools that"
    echo "create directory markers (like aws s3 sync, rclone, etc.)"
    echo ""
    echo "✗✗✗ TEST FAILED: Directories not visible after remount"
    exit 1
fi
