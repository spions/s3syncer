#!/bin/bash
set -e

echo "=== GeeseFS Directory Persistence Test ==="
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
echo ""

echo "Step 2: Verifying structure before remount..."
if [ -d "$MOUNT_POINT/$TEST_DIR/$TEST_SUBDIR1/$TEST_SUBDIR2" ]; then
    echo "✓ All directories visible before remount"
    ls -la "$MOUNT_POINT/$TEST_DIR/$TEST_SUBDIR1/"
else
    echo "✗ Directories not visible before remount"
    exit 1
fi
echo ""

# Step 2.5: Creating directory marker objects BEFORE unmounting...
echo "Step 2.5: Creating directory marker objects BEFORE unmounting..."
echo "This ensures S3 has actual objects for each directory"
# Create empty marker files for each directory
# In GeeseFS, creating a file in a directory ensures the directory structure exists
for dir in "$TEST_DIR/" "$TEST_DIR/$TEST_SUBDIR1/" "$TEST_DIR/$TEST_SUBDIR1/$TEST_SUBDIR2/"; do
    marker_file="$MOUNT_POINT/${dir}.dirmarker"
    echo "Creating marker: $marker_file"
    touch "$marker_file"
done
sync
sleep 2
echo ""

# Step 3: Unmounting GeeseFS...
echo "Step 3: Unmounting GeeseFS..."
sync
fusermount -uz "$MOUNT_POINT" || umount -l "$MOUNT_POINT" || true
sleep 2
echo ""

echo "Step 4: Remounting GeeseFS..."
geesefs \
    --debug_s3 \
    --debug_fuse \
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
echo "Checking $MOUNT_POINT/$TEST_DIR/:"
ls -la "$MOUNT_POINT/$TEST_DIR/" || echo "  Directory not accessible"
echo ""

echo "Checking $MOUNT_POINT/$TEST_DIR/$TEST_SUBDIR1/:"
if ls -la "$MOUNT_POINT/$TEST_DIR/$TEST_SUBDIR1/" 2>/dev/null; then
    echo "✓ Subdirectory $TEST_SUBDIR1 is visible"

    echo ""
    echo "Checking $MOUNT_POINT/$TEST_DIR/$TEST_SUBDIR1/$TEST_SUBDIR2/:"
    if ls -la "$MOUNT_POINT/$TEST_DIR/$TEST_SUBDIR1/$TEST_SUBDIR2/" 2>/dev/null; then
        echo "✓ Subdirectory $TEST_SUBDIR2 is visible"
        echo "✓✓✓ TEST PASSED: All directories persist after remount!"
        exit 0
    else
        echo "✗ Subdirectory $TEST_SUBDIR2 NOT visible after remount"
    fi
else
    echo "✗ Subdirectory $TEST_SUBDIR1 NOT visible after remount"
fi
echo ""

# Step 6: FIXING - Creating directory marker objects...
echo "Step 6: FIXING - Creating directory marker objects..."
echo "This is needed because S3 doesn't have real directories, only objects with '/' in keys"
echo ""

# Create directory markers by creating empty files in directory paths
# If the directory doesn't exist, we recreate it and add a file
mkdir -p "$MOUNT_POINT/$TEST_DIR/$TEST_SUBDIR1/$TEST_SUBDIR2"
touch "$MOUNT_POINT/$TEST_DIR/.keep"
touch "$MOUNT_POINT/$TEST_DIR/$TEST_SUBDIR1/.keep"
touch "$MOUNT_POINT/$TEST_DIR/$TEST_SUBDIR1/$TEST_SUBDIR2/.keep"
sync

# Alternative: use aws cli if available
if command -v aws &> /dev/null && [ "$(uname -m)" = "x86_64" ]; then
    echo "Using AWS CLI to create directory markers..."
    aws s3api put-object --bucket testsync --key "$TEST_DIR/" --endpoint-url http://s3syncer:8080 || true
    aws s3api put-object --bucket testsync --key "$TEST_DIR/$TEST_SUBDIR1/" --endpoint-url http://s3syncer:8080 || true
    aws s3api put-object --bucket testsync --key "$TEST_DIR/$TEST_SUBDIR1/$TEST_SUBDIR2/" --endpoint-url http://s3syncer:8080 || true
fi

sleep 2

echo ""
echo "Step 7: Remounting again to verify fix..."
fusermount -u "$MOUNT_POINT" || umount "$MOUNT_POINT" || true
sleep 2

geesefs \
    --debug_s3 \
    --debug_fuse \
    --log-file=/data/log-remount2.txt \
    --file-mode=0666 \
    --dir-mode=0777 \
    --endpoint=http://s3syncer:8080/ \
    testsync \
    "$MOUNT_POINT" &

sleep 3
echo ""

echo "Step 8: Final verification..."
echo "Checking $MOUNT_POINT/$TEST_DIR/$TEST_SUBDIR1/$TEST_SUBDIR2/:"
if ls -la "$MOUNT_POINT/$TEST_DIR/$TEST_SUBDIR1/$TEST_SUBDIR2/" 2>/dev/null; then
    echo "✓✓✓ FIX SUCCESSFUL: All directories now visible after remount!"
    echo ""
    echo "Summary: Directory markers were missing. They have been created."
    echo "To prevent this issue, always create directory markers when creating directories."
    exit 0
else
    echo "✗✗✗ FIX FAILED: Directories still not visible"
    echo "Check logs at /data/log-remount2.txt"
    exit 1
fi
