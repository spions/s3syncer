#!/bin/bash

# Simple test to check what's returned in XML
echo "Mounting GeeseFS..."
geesefs \
    --file-mode=0666 \
    --dir-mode=0777 \
    --endpoint=http://s3syncer:8080/ \
    testsync \
    /mnt/testsync &

sleep 3

echo "Listing root:"
ls -la /mnt/testsync/ | grep -E "^d|^-" | grep -v "^\."

echo ""
echo "Checking if test/ directory exists:"
if [ -d "/mnt/testsync/test" ]; then
    echo "✓ test/ directory IS visible"
    ls -la /mnt/testsync/test/
else
    echo "✗ test/ directory NOT visible"
fi

echo ""
echo "All files and directories in root:"
ls -1 /mnt/testsync/
