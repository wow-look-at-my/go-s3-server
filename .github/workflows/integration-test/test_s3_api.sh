#!/usr/bin/env bash
# Integration tests for go-s3-server using curl with HTTP Basic Auth.
set -euo pipefail

ENDPOINT_NORMAL="http://127.0.0.1:9000"
ENDPOINT_WRITEONCE="http://127.0.0.1:9001"
BUCKET="test-cache"
DATA_DIR_NORMAL="/tmp/s3-data"
DATA_DIR_WRITEONCE="/tmp/s3-data-writeonce"
AUTH_USER="test-key-id"
AUTH_PASS="test-secret-key"
AUTH="$AUTH_USER:$AUTH_PASS"

PASS=0
FAIL=0

pass() { PASS=$((PASS + 1)); echo "  PASS: $1"; }
fail() { FAIL=$((FAIL + 1)); echo "  FAIL: $1"; }

# ── S3 API Tests (normal server) ────────────────────────────────────────────

echo "=== S3 API Tests ==="

# PutObject + GetObject roundtrip
echo -n "hello world cache data" > /tmp/test-upload
curl -sf -u "$AUTH" -X PUT --data-binary @/tmp/test-upload \
  "$ENDPOINT_NORMAL/$BUCKET/api/v1test000000000001" > /dev/null
curl -sf -u "$AUTH" -o /tmp/test-download \
  "$ENDPOINT_NORMAL/$BUCKET/api/v1test000000000001"
if diff -q /tmp/test-upload /tmp/test-download > /dev/null 2>&1; then
  pass "PutObject + GetObject roundtrip"
else
  fail "PutObject + GetObject roundtrip: content mismatch"
fi

# GetObject 404
HTTP_CODE=$(curl -s -u "$AUTH" -o /tmp/test-404 -w "%{http_code}" \
  "$ENDPOINT_NORMAL/$BUCKET/nonexistent/v1xxxx000000000000")
if [ "$HTTP_CODE" = "404" ]; then
  pass "GetObject nonexistent returns 404"
else
  fail "GetObject nonexistent returned $HTTP_CODE, expected 404"
fi

# ListObjectsV2
for i in 0 1 2; do
  curl -sf -u "$AUTH" -X PUT --data-binary "data" \
    "$ENDPOINT_NORMAL/$BUCKET/listtest/v1aaaa$(printf '%012d' $i)" > /dev/null
done
LIST_BODY=$(curl -sf -u "$AUTH" "$ENDPOINT_NORMAL/$BUCKET?list-type=2&prefix=listtest/")
LIST_COUNT=$(echo "$LIST_BODY" | grep -o '<Key>' | wc -l)
if [ "$LIST_COUNT" = "3" ]; then
  pass "ListObjectsV2 returns 3 objects"
else
  fail "ListObjectsV2 expected 3 objects, got $LIST_COUNT"
fi

# ListObjectsV2 pagination
for i in $(seq 0 4); do
  curl -sf -u "$AUTH" -X PUT --data-binary "d" \
    "$ENDPOINT_NORMAL/$BUCKET/pagtest/v1aaaa$(printf '%012d' $i)" > /dev/null
done
PAGE1=$(curl -sf -u "$AUTH" "$ENDPOINT_NORMAL/$BUCKET?list-type=2&prefix=pagtest/&max-keys=2")
PAGE1_COUNT=$(echo "$PAGE1" | grep -o '<Key>' | wc -l)
PAGE1_TRUNCATED=$(echo "$PAGE1" | grep -c '<IsTruncated>true</IsTruncated>' || true)
if [ "$PAGE1_COUNT" = "2" ] && [ "$PAGE1_TRUNCATED" = "1" ]; then
  pass "ListObjectsV2 pagination page 1"
else
  fail "ListObjectsV2 pagination page 1: count=$PAGE1_COUNT truncated=$PAGE1_TRUNCATED"
fi

# Overwrite allowed in normal mode
curl -sf -u "$AUTH" -X PUT --data-binary "first" \
  "$ENDPOINT_NORMAL/$BUCKET/overwrite/v1test000000000002" > /dev/null
curl -sf -u "$AUTH" -X PUT --data-binary "second" \
  "$ENDPOINT_NORMAL/$BUCKET/overwrite/v1test000000000002" > /dev/null
curl -sf -u "$AUTH" -o /tmp/test-ow-get \
  "$ENDPOINT_NORMAL/$BUCKET/overwrite/v1test000000000002"
if [ "$(cat /tmp/test-ow-get)" = "second" ]; then
  pass "Overwrite allowed in normal mode"
else
  fail "Overwrite allowed: expected 'second', got '$(cat /tmp/test-ow-get)'"
fi

# Sharded storage verification
curl -sf -u "$AUTH" -X PUT --data-binary "sharddata" \
  "$ENDPOINT_NORMAL/$BUCKET/shardtest/v1aabbccdd11223344" > /dev/null
SHARD_PATH="$DATA_DIR_NORMAL/shardtest/v1/aa/bbccdd11223344"
if [ -f "$SHARD_PATH" ] && [ "$(cat "$SHARD_PATH")" = "sharddata" ]; then
  pass "Sharded storage: file at $SHARD_PATH"
else
  fail "Sharded storage: expected file at $SHARD_PATH"
fi

# Metadata roundtrip
curl -sf -u "$AUTH" -X PUT --data-binary "metabody" \
  -H "X-Amz-Meta-Outputid: abc123" -H "X-Amz-Meta-Custom: val2" \
  "$ENDPOINT_NORMAL/$BUCKET/metatest/v1meta000000000001" > /dev/null
curl -sf -u "$AUTH" -D /tmp/test-meta-headers -o /tmp/test-meta-get \
  "$ENDPOINT_NORMAL/$BUCKET/metatest/v1meta000000000001"
META_OUTPUTID=$(grep -i 'X-Amz-Meta-Outputid' /tmp/test-meta-headers | tr -d '\r' | awk '{print $2}')
if [ "$META_OUTPUTID" = "abc123" ]; then
  pass "Metadata roundtrip"
else
  fail "Metadata roundtrip: outputid='$META_OUTPUTID'"
fi

# ── Auth Tests ──────────────────────────────────────────────────────────────

echo ""
echo "=== Auth Tests ==="

# Invalid secret key
HTTP_CODE=$(curl -s -u "$AUTH_USER:wrongsecret" -o /dev/null -w "%{http_code}" \
  "$ENDPOINT_NORMAL/$BUCKET/auth/v1test000000000001")
if [ "$HTTP_CODE" = "403" ]; then
  pass "Invalid secret key returns 403"
else
  fail "Invalid secret key returned $HTTP_CODE, expected 403"
fi

# Invalid access key
HTTP_CODE=$(curl -s -u "NONEXISTENT:$AUTH_PASS" -o /dev/null -w "%{http_code}" \
  "$ENDPOINT_NORMAL/$BUCKET/auth/v1test000000000002")
if [ "$HTTP_CODE" = "403" ]; then
  pass "Invalid access key returns 403"
else
  fail "Invalid access key returned $HTTP_CODE, expected 403"
fi

# No auth header (raw curl)
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "$ENDPOINT_NORMAL/$BUCKET/auth/v1test000000000003")
if [ "$HTTP_CODE" = "403" ]; then
  pass "No auth header returns 403"
else
  fail "No auth header returned $HTTP_CODE, expected 403"
fi

# ── Write-Once Tests ────────────────────────────────────────────────────────

echo ""
echo "=== Write-Once Tests ==="

# First PUT succeeds
curl -sf -u "$AUTH" -X PUT --data-binary "first write" \
  "$ENDPOINT_WRITEONCE/$BUCKET/wo/v1first000000000001" > /dev/null
curl -sf -u "$AUTH" -o /tmp/test-wo1-get \
  "$ENDPOINT_WRITEONCE/$BUCKET/wo/v1first000000000001"
if [ "$(cat /tmp/test-wo1-get)" = "first write" ]; then
  pass "Write-once: first PUT succeeds"
else
  fail "Write-once: first PUT content mismatch"
fi

# Same content is idempotent
curl -sf -u "$AUTH" -X PUT --data-binary "idempotent content" \
  "$ENDPOINT_WRITEONCE/$BUCKET/wo/v1idempotent0000001" > /dev/null
HTTP_CODE=$(curl -s -u "$AUTH" -X PUT --data-binary "idempotent content" -o /dev/null -w "%{http_code}" \
  "$ENDPOINT_WRITEONCE/$BUCKET/wo/v1idempotent0000001")
if [ "$HTTP_CODE" = "200" ]; then
  pass "Write-once: same content idempotent"
else
  fail "Write-once: same content PUT returned $HTTP_CODE, expected 200"
fi

# Different content returns 409
curl -sf -u "$AUTH" -X PUT --data-binary "original" \
  "$ENDPOINT_WRITEONCE/$BUCKET/wo/v1conflict000000001" > /dev/null
HTTP_CODE=$(curl -s -u "$AUTH" -X PUT --data-binary "different" -o /dev/null -w "%{http_code}" \
  "$ENDPOINT_WRITEONCE/$BUCKET/wo/v1conflict000000001")
if [ "$HTTP_CODE" = "409" ]; then
  pass "Write-once: different content returns 409"
else
  fail "Write-once: different content returned $HTTP_CODE, expected 409"
fi

# Original content preserved after conflict
curl -sf -u "$AUTH" -o /tmp/test-wo-preserved \
  "$ENDPOINT_WRITEONCE/$BUCKET/wo/v1conflict000000001"
if [ "$(cat /tmp/test-wo-preserved)" = "original" ]; then
  pass "Write-once: original content preserved after conflict"
else
  fail "Write-once: expected 'original', got '$(cat /tmp/test-wo-preserved)'"
fi

# Sharded storage in write-once mode
curl -sf -u "$AUTH" -X PUT --data-binary "wosharddata" \
  "$ENDPOINT_WRITEONCE/$BUCKET/woshard/v1aabbccdd99887766" > /dev/null
WO_SHARD_PATH="$DATA_DIR_WRITEONCE/woshard/v1/aa/bbccdd99887766"
if [ -f "$WO_SHARD_PATH" ]; then
  pass "Write-once: sharded storage"
else
  fail "Write-once: sharded file not found at $WO_SHARD_PATH"
fi

# ── Summary ─────────────────────────────────────────────────────────────────

echo ""
echo "════════════════════════════════════════"
echo "Results: $PASS passed, $FAIL failed"
echo "════════════════════════════════════════"

if [ "$FAIL" -gt 0 ]; then
  exit 1
fi
