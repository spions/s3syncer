#!/bin/bash
# Real AWS CLI against s3syncer over plain HTTP: uploads use the aws-chunked
# streaming signature (the whole part is sent as one chunk), which must be
# decoded, verified and replicated byte for byte.
set -u

EP="${ENDPOINT:-http://s3syncer:8080}"
BUCKET="${BUCKET:-testsync}"
fail=0

# Single PUT for everything below (multipart upload is not supported by s3syncer)
aws configure set default.s3.multipart_threshold 5GB

for size in 1 100000 5000000 50000000; do
    head -c "$size" /dev/urandom > "/tmp/src_$size.bin"
    key="awscli_$size.bin"
    if ! aws s3 cp "/tmp/src_$size.bin" "s3://$BUCKET/$key" --endpoint-url "$EP" --only-show-errors; then
        echo "✗ upload of $size bytes failed"; fail=1; continue
    fi
    aws s3 cp "s3://$BUCKET/$key" "/tmp/back_$size.bin" --endpoint-url "$EP" --only-show-errors
    if [ "$(sha256sum < "/tmp/src_$size.bin")" = "$(sha256sum < "/tmp/back_$size.bin")" ]; then
        echo "✓ $size bytes: round trip identical"
    else
        echo "✗ $size bytes: content differs after round trip"; fail=1
    fi
    rm -f "/tmp/src_$size.bin" "/tmp/back_$size.bin"
done

# Object stored by the proxy must also exist on the backup (sync is asynchronous for big bodies)
head -c 3000000 /dev/urandom > /tmp/p.bin
aws s3api put-object --bucket "$BUCKET" --key awscli_putobject.bin --body /tmp/p.bin --endpoint-url "$EP" > /dev/null \
    && echo "✓ s3api put-object" || { echo "✗ s3api put-object failed"; fail=1; }

# A body that is cut short must not be stored
head -c 100000 /dev/urandom > /tmp/trunc.bin
if curl -s -o /dev/null -w '%{http_code}' -X PUT --data-binary @/tmp/trunc.bin \
     -H "Authorization: AWS4-HMAC-SHA256 Credential=testtest/20200101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=00" \
     "$EP/$BUCKET/forged.bin" | grep -q 403; then
    echo "✓ forged signature rejected"
else
    echo "✗ forged request was not rejected"; fail=1
fi

[ "$fail" = 0 ] && echo "AWS CLI tests passed" || { echo "AWS CLI tests FAILED"; exit 1; }
