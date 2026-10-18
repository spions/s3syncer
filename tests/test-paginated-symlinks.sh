#!/bin/bash
set -e

echo "=== Paginated Symlink Test ==="
echo ""

MOUNT_POINT="/mnt/testsync"
FILE_COUNT=2000
PAGE_SIZE=500

# Mount GeeseFS
echo "Step 1: Mounting GeeseFS..."
geesefs \
    --debug_s3 \
    --debug_fuse \
    --log-file=/data/paginated-log.txt \
    --file-mode=0666 \
    --dir-mode=0777 \
    --list-type=ext-v1 \
    --endpoint=http://s3syncer:8080/ \
    testsync \
    "$MOUNT_POINT" &

GEESEFS_PID=$!
sleep 3

if ! mountpoint -q "$MOUNT_POINT"; then
    echo "✗ Failed to mount GeeseFS"
    exit 1
fi
echo "✓ GeeseFS mounted"

# Create many files and some symlinks
echo "Step 2: Creating $FILE_COUNT files, directories and some symlinks..."

# Clean up any existing files if any
rm -rf "$MOUNT_POINT"/*

# Create directories
mkdir -p "$MOUNT_POINT/dir1/subdir1"
mkdir -p "$MOUNT_POINT/dir2"

# Create many files
for i in $(seq -f "%04g" 1 $FILE_COUNT); do
    echo "content $i" > "$MOUNT_POINT/file_$i.txt"
done

# Create files in subdirectories
echo "content in dir1" > "$MOUNT_POINT/dir1/file_in_dir1.txt"
echo "content in subdir1" > "$MOUNT_POINT/dir1/subdir1/file_in_subdir1.txt"

# Create symlinks in root
ln -s file_0001.txt "$MOUNT_POINT/link_0001.txt"
ln -s dir1/file_in_dir1.txt "$MOUNT_POINT/link_to_dir1_file.txt"

# Create symlinks in subdirectories
ln -s ../file_0001.txt "$MOUNT_POINT/dir1/link_to_root_file.txt"
ln -s file_in_dir1.txt "$MOUNT_POINT/dir1/link_local.txt"
ln -s subdir1/file_in_subdir1.txt "$MOUNT_POINT/dir1/link_to_subdir_file.txt"

sync
sleep 2
echo "✓ Files, directories and symlinks created"

# Unmount and remount to force listing from S3
echo "Step 3: Remounting GeeseFS..."
fusermount -uz "$MOUNT_POINT" || umount -l "$MOUNT_POINT" || true
sleep 2

geesefs \
    --debug_s3 \
    --debug_fuse \
    --log-file=/data/paginated-log-remount.txt \
    --file-mode=0666 \
    --dir-mode=0777 \
    --list-type=ext-v1 \
    --endpoint=http://s3syncer:8080/ \
    testsync \
    "$MOUNT_POINT" &

sleep 3
echo "✓ GeeseFS remounted"

# Verify symlinks in root
echo "Step 4: Verifying symlinks via ls -la..."
LS_OUT=$(ls -la "$MOUNT_POINT")
echo "$LS_OUT"

LINKS_FOUND=$(echo "$LS_OUT" | grep "^l" | wc -l)
echo "Found $LINKS_FOUND symlinks in root"

if [ "$LINKS_FOUND" -lt 2 ]; then
    echo "✗ MISSING SYMLINKS IN ROOT!"
    exit 1
fi

# Verify symlinks in subdirectory dir1
echo "Verifying symlinks in dir1..."
LS_DIR1=$(ls -la "$MOUNT_POINT/dir1")
echo "$LS_DIR1"

LINKS_IN_DIR1=$(echo "$LS_DIR1" | grep "^l" | wc -l)
echo "Found $LINKS_IN_DIR1 symlinks in dir1"

if [ "$LINKS_IN_DIR1" -lt 3 ]; then
    echo "✗ MISSING SYMLINKS IN dir1!"
    exit 1
fi

echo "✓ All symlinks correctly identified"

# Cleanup
fusermount -uz "$MOUNT_POINT" || umount -l "$MOUNT_POINT" || true

# Step 5: Testing resync with symlinks
echo "Step 5: Testing resync with symlinks..."
# We need to trigger resync via API
curl -X POST -H "Authorization: Bearer ${S3SYNCER_API_TOKEN:-testtest}" "http://s3syncer:8080/api/tasks/resync?bucket=testsync"
echo ""
echo "Waiting for resync to complete..."
# Poll resync status until completed (up to 120s)
for i in $(seq 1 120); do
    TASKS=$(curl -sL -H "Authorization: Bearer ${S3SYNCER_API_TOKEN:-testtest}" http://s3syncer:8080/api/tasks/)
    if echo "$TASKS" | grep -q '"status":"completed"'; then
        echo "Resync completed"
        break
    fi
    sleep 1
done
sleep 5

# Step 6: Verifying symlinks in backup (testsync2) via mc stat
echo "Step 6: Verifying symlinks in backup (testsync2)..."

# Setup mc alias for minio2
mc alias set backup http://minio2:9000 testtest testtest > /dev/null 2>&1

SYMLINKS_OK=0

check_symlink_meta() {
    local KEY="$1"
    local STAT
    STAT=$(mc stat "backup/testsync2/$KEY" 2>&1 || true)
    if echo "$STAT" | grep -qi "symlink-target"; then
        echo "✓ $KEY has symlink metadata"
        return 0
    else
        echo "✗ $KEY missing symlink metadata"
        echo "  Stat: $STAT"
        return 1
    fi
}

echo "Checking link_0001.txt in backup..."
if check_symlink_meta "link_0001.txt"; then
    SYMLINKS_OK=$((SYMLINKS_OK + 1))
fi

echo "Checking link_to_dir1_file.txt in backup..."
if check_symlink_meta "link_to_dir1_file.txt"; then
    SYMLINKS_OK=$((SYMLINKS_OK + 1))
fi

if [ "$SYMLINKS_OK" -lt 2 ]; then
    echo "✗ SYMLINKS LOST IN ROOT OF BACKUP! Expected at least 2, got $SYMLINKS_OK"
    exit 1
fi
echo "Found $SYMLINKS_OK symlinks in root of backup"

# Check dir1 symlinks
DIR1_SYMLINKS=0
for KEY in dir1/link_to_root_file.txt dir1/link_local.txt dir1/link_to_subdir_file.txt; do
    echo "Checking $KEY in backup..."
    if check_symlink_meta "$KEY"; then
        DIR1_SYMLINKS=$((DIR1_SYMLINKS + 1))
    fi
done

echo "Found $DIR1_SYMLINKS symlinks in dir1 of backup"
if [ "$DIR1_SYMLINKS" -lt 3 ]; then
    echo "✗ SYMLINKS LOST IN dir1 OF BACKUP! Expected at least 3, got $DIR1_SYMLINKS"
    exit 1
fi

echo "✓ All symlinks preserved in backup after resync"
echo "=== Test Passed! ==="
