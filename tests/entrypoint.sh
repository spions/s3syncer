#!/bin/bash
set -e

echo "Starting GeeseFS mount..."

# Mount GeeseFS with debug logging
geesefs \
    --debug_s3 \
    --debug_fuse \
    --log-file=/data/log.txt \
    --file-mode=0666 \
    --dir-mode=0777 \
    --endpoint=http://s3syncer:8080/ \
    testsync \
    /mnt/testsync &

GEESEFS_PID=$!

# Wait for mount to be ready
sleep 3

echo "GeeseFS mounted, running tests..."

# Cleanup old test files
echo "Cleaning up old test files..."
rm -f /mnt/testsync/test_file.txt /mnt/testsync/test_symlink.txt
sleep 1

# Test 1: List directory
echo "Test 1: List directory"
if ls -la /mnt/testsync; then
    echo "✓ List directory successful"
else
    echo "✗ List directory failed"
    cat /data/log.txt
    exit 1
fi

# Test 2: Create a regular file
echo ""
echo "Test 2: Create regular file"
if echo "test content" > /mnt/testsync/test_file.txt; then
    echo "✓ Create file successful"
    sleep 2
    if ls -la /mnt/testsync; then
        echo "✓ Directory still accessible after file creation"
    else
        echo "✗ Directory not accessible after file creation"
        cat /data/log.txt
        exit 1
    fi
else
    echo "✗ Create file failed"
    cat /data/log.txt
    exit 1
fi

# Test 3: Read the file back
echo ""
echo "Test 3: Read file"
if cat /mnt/testsync/test_file.txt; then
    echo "✓ Read file successful"
else
    echo "✗ Read file failed"
    cat /data/log.txt
    exit 1
fi

# Test 4: Create a symlink
echo ""
echo "Test 4: Create symlink"
if ln -s test_file.txt /mnt/testsync/test_symlink.txt; then
    echo "✓ Create symlink successful"
    sleep 2
    if ls -la /mnt/testsync; then
        echo "✓ Directory still accessible after symlink creation"
    else
        echo "✗ Directory not accessible after symlink creation"
        cat /data/log.txt
        exit 1
    fi
else
    echo "✗ Create symlink failed"
    cat /data/log.txt
    exit 1
fi

# Test 5: Read through symlink
echo ""
echo "Test 5: Read through symlink"
if cat /mnt/testsync/test_symlink.txt; then
    echo "✓ Read through symlink successful"
else
    echo "✗ Read through symlink failed"
    cat /data/log.txt
    exit 1
fi

echo ""
echo "All tests passed!"
echo ""
echo "Logs are available in /data/log.txt"

# Set KEEP_RUNNING=1 to keep the mount alive for manual inspection
if [ -n "$KEEP_RUNNING" ]; then
    wait $GEESEFS_PID
fi
