#!/usr/bin/env bash
# Integration tests for go-s3-server using the aws CLI.
# Requires: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION set.
set -euo pipefail

ENDPOINT_NORMAL="http://127.0.0.1:9000"
ENDPOINT_WRITEONCE="http://127.0.0.1:9001"
BUCKET="test-cache"
DATA_DIR_NORMAL="/tmp/s3-data"
DATA_DIR_WRITEONCE="/tmp/s3-data-writeonce"

PASS=0
FAIL=0

pass() { PASS=$((PASS + 1)); echo "  PASS: $1"; }
fail() { FAIL=$((FAIL + 1)); echo "  FAIL: $1"; }

# ── S3 API Tests (normal server) ────────────────────────────────────────────

echo "=== S3 API Tests ==="

# PutObject + GetObject roundtrip
echo -n "hello world cache data" > /tmp/test-upload
aws s3api put-object --endpoint-url "$ENDPOINT_NORMAL" --bucket "$BUCKET" \
  --key "api/v1test000000000001" --body /tmp/test-upload --no-cli-pager > /dev/null 2>&1
aws s3api get-object --endpoint-url "$ENDPOINT_NORMAL" --bucket "$BUCKET" \
  --key "api/v1test000000000001" /tmp/test-download --no-cli-pager > /dev/null 2>&1
if diff -q /tmp/test-upload /tmp/test-download > /dev/null 2>&1; then
  pass "PutObject + GetObject roundtrip"
else
  fail "PutObject + GetObject roundtrip: content mismatch"
fi

# GetObject 404
if aws s3api get-object --endpoint-url "$ENDPOINT_NORMAL" --bucket "$BUCKET" \
  --key "nonexistent/v1xxxx000000000000" /tmp/test-404 --no-cli-pager 2>&1 | grep -q "NoSuchKey"; then
  pass "GetObject nonexistent returns NoSuchKey"
else
  fail "GetObject nonexistent did not return NoSuchKey"
fi

# ListObjectsV2
for i in 0 1 2; do
  echo -n "data" > /tmp/test-list-item
  aws s3api put-object --endpoint-url "$ENDPOINT_NORMAL" --bucket "$BUCKET" \
    --key "listtest/v1aaaa$(printf '%012d' $i)" --body /tmp/test-list-item --no-cli-pager > /dev/null 2>&1
done
LIST_COUNT=$(aws s3api list-objects-v2 --endpoint-url "$ENDPOINT_NORMAL" --bucket "$BUCKET" \
  --prefix "listtest/" --no-cli-pager 2>/dev/null | jq '.Contents | length')
if [ "$LIST_COUNT" = "3" ]; then
  pass "ListObjectsV2 returns 3 objects"
else
  fail "ListObjectsV2 expected 3 objects, got $LIST_COUNT"
fi

# ListObjectsV2 pagination
for i in $(seq 0 4); do
  echo -n "d" > /tmp/test-pag-item
  aws s3api put-object --endpoint-url "$ENDPOINT_NORMAL" --bucket "$BUCKET" \
    --key "pagtest/v1aaaa$(printf '%012d' $i)" --body /tmp/test-pag-item --no-cli-pager > /dev/null 2>&1
done
PAGE1=$(aws s3api list-objects-v2 --endpoint-url "$ENDPOINT_NORMAL" --bucket "$BUCKET" \
  --prefix "pagtest/" --max-keys 2 --no-cli-pager 2>/dev/null)
PAGE1_COUNT=$(echo "$PAGE1" | jq '.Contents | length')
PAGE1_TRUNCATED=$(echo "$PAGE1" | jq '.IsTruncated')
if [ "$PAGE1_COUNT" = "2" ] && [ "$PAGE1_TRUNCATED" = "true" ]; then
  pass "ListObjectsV2 pagination page 1"
else
  fail "ListObjectsV2 pagination page 1: count=$PAGE1_COUNT truncated=$PAGE1_TRUNCATED"
fi

# Overwrite allowed in normal mode
echo -n "first" > /tmp/test-ow1
echo -n "second" > /tmp/test-ow2
aws s3api put-object --endpoint-url "$ENDPOINT_NORMAL" --bucket "$BUCKET" \
  --key "overwrite/v1test000000000002" --body /tmp/test-ow1 --no-cli-pager > /dev/null 2>&1
aws s3api put-object --endpoint-url "$ENDPOINT_NORMAL" --bucket "$BUCKET" \
  --key "overwrite/v1test000000000002" --body /tmp/test-ow2 --no-cli-pager > /dev/null 2>&1
aws s3api get-object --endpoint-url "$ENDPOINT_NORMAL" --bucket "$BUCKET" \
  --key "overwrite/v1test000000000002" /tmp/test-ow-get --no-cli-pager > /dev/null 2>&1
if diff -q /tmp/test-ow2 /tmp/test-ow-get > /dev/null 2>&1; then
  pass "Overwrite allowed in normal mode"
else
  fail "Overwrite allowed: expected 'second', got '$(cat /tmp/test-ow-get)'"
fi

# Sharded storage verification
echo -n "sharddata" > /tmp/test-shard
aws s3api put-object --endpoint-url "$ENDPOINT_NORMAL" --bucket "$BUCKET" \
  --key "shardtest/v1aabbccdd11223344" --body /tmp/test-shard --no-cli-pager > /dev/null 2>&1
SHARD_PATH="$DATA_DIR_NORMAL/shardtest/v1/aa/bbccdd11223344"
if [ -f "$SHARD_PATH" ] && [ "$(cat "$SHARD_PATH")" = "sharddata" ]; then
  pass "Sharded storage: file at $SHARD_PATH"
else
  fail "Sharded storage: expected file at $SHARD_PATH"
fi

# Metadata roundtrip
echo -n "metabody" > /tmp/test-meta
aws s3api put-object --endpoint-url "$ENDPOINT_NORMAL" --bucket "$BUCKET" \
  --key "metatest/v1meta000000000001" --body /tmp/test-meta \
  --metadata '{"outputid":"abc123","custom":"val2"}' --no-cli-pager > /dev/null 2>&1
META_RESP=$(aws s3api get-object --endpoint-url "$ENDPOINT_NORMAL" --bucket "$BUCKET" \
  --key "metatest/v1meta000000000001" /tmp/test-meta-get --no-cli-pager 2>/dev/null)
META_OUTPUTID=$(echo "$META_RESP" | jq -r '.Metadata.Outputid // .Metadata.outputid // empty')
if [ "$META_OUTPUTID" = "abc123" ]; then
  pass "Metadata roundtrip"
else
  fail "Metadata roundtrip: outputid='$META_OUTPUTID'"
fi

# ── Auth Tests ──────────────────────────────────────────────────────────────

echo ""
echo "=== Auth Tests ==="

# Invalid secret key
if AWS_SECRET_ACCESS_KEY=wrongsecret aws s3api get-object --endpoint-url "$ENDPOINT_NORMAL" \
  --bucket "$BUCKET" --key "auth/v1test000000000001" /tmp/test-auth1 --no-cli-pager 2>&1 | grep -q "403\|AccessDenied"; then
  pass "Invalid secret key returns 403"
else
  fail "Invalid secret key did not return 403"
fi

# Invalid access key
if AWS_ACCESS_KEY_ID=NONEXISTENT aws s3api get-object --endpoint-url "$ENDPOINT_NORMAL" \
  --bucket "$BUCKET" --key "auth/v1test000000000002" /tmp/test-auth2 --no-cli-pager 2>&1 | grep -q "403\|AccessDenied"; then
  pass "Invalid access key returns 403"
else
  fail "Invalid access key did not return 403"
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
echo -n "first write" > /tmp/test-wo1
aws s3api put-object --endpoint-url "$ENDPOINT_WRITEONCE" --bucket "$BUCKET" \
  --key "wo/v1first000000000001" --body /tmp/test-wo1 --no-cli-pager > /dev/null 2>&1
aws s3api get-object --endpoint-url "$ENDPOINT_WRITEONCE" --bucket "$BUCKET" \
  --key "wo/v1first000000000001" /tmp/test-wo1-get --no-cli-pager > /dev/null 2>&1
if diff -q /tmp/test-wo1 /tmp/test-wo1-get > /dev/null 2>&1; then
  pass "Write-once: first PUT succeeds"
else
  fail "Write-once: first PUT content mismatch"
fi

# Same content is idempotent
echo -n "idempotent content" > /tmp/test-wo-idem
aws s3api put-object --endpoint-url "$ENDPOINT_WRITEONCE" --bucket "$BUCKET" \
  --key "wo/v1idempotent0000001" --body /tmp/test-wo-idem --no-cli-pager > /dev/null 2>&1
# Second PUT with same content should succeed (exit 0)
if aws s3api put-object --endpoint-url "$ENDPOINT_WRITEONCE" --bucket "$BUCKET" \
  --key "wo/v1idempotent0000001" --body /tmp/test-wo-idem --no-cli-pager > /dev/null 2>&1; then
  pass "Write-once: same content idempotent"
else
  fail "Write-once: same content PUT should have succeeded"
fi

# Different content returns 409
echo -n "original" > /tmp/test-wo-orig
echo -n "different" > /tmp/test-wo-diff
aws s3api put-object --endpoint-url "$ENDPOINT_WRITEONCE" --bucket "$BUCKET" \
  --key "wo/v1conflict000000001" --body /tmp/test-wo-orig --no-cli-pager > /dev/null 2>&1
if aws s3api put-object --endpoint-url "$ENDPOINT_WRITEONCE" --bucket "$BUCKET" \
  --key "wo/v1conflict000000001" --body /tmp/test-wo-diff --no-cli-pager 2>&1 | grep -q "409\|ConflictException"; then
  pass "Write-once: different content returns 409"
else
  fail "Write-once: different content did not return 409"
fi

# Original content preserved after conflict
aws s3api get-object --endpoint-url "$ENDPOINT_WRITEONCE" --bucket "$BUCKET" \
  --key "wo/v1conflict000000001" /tmp/test-wo-preserved --no-cli-pager > /dev/null 2>&1
if [ "$(cat /tmp/test-wo-preserved)" = "original" ]; then
  pass "Write-once: original content preserved after conflict"
else
  fail "Write-once: expected 'original', got '$(cat /tmp/test-wo-preserved)'"
fi

# Sharded storage in write-once mode
echo -n "wosharddata" > /tmp/test-wo-shard
aws s3api put-object --endpoint-url "$ENDPOINT_WRITEONCE" --bucket "$BUCKET" \
  --key "woshard/v1aabbccdd99887766" --body /tmp/test-wo-shard --no-cli-pager > /dev/null 2>&1
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
