#!/usr/bin/env bash
# Integration tests for go-s3-server using curl with HTTP Basic Auth.
set -euo pipefail

ENDPOINT_NORMAL="http://127.0.0.1:9000"
ENDPOINT_WRITEONCE="http://127.0.0.1:9001"
BUCKET="test-cache"
DATA_DIR_NORMAL="/tmp/s3-data"
DATA_DIR_WRITEONCE="/tmp/s3-data-writeonce"
AUTH="testuser:testpass"

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

# GetObject 404 — native plain-text error ("<code>: <message>"), no S3 XML.
HTTP_CODE=$(curl -s -o /tmp/test-404-body -w "%{http_code}" -u "$AUTH" \
  "$ENDPOINT_NORMAL/$BUCKET/nonexistent/v1xxxx000000000000")
if [ "$HTTP_CODE" = "404" ] && grep -q "not_found" /tmp/test-404-body; then
  pass "GetObject nonexistent returns not_found"
else
  fail "GetObject nonexistent: code=$HTTP_CODE body=$(cat /tmp/test-404-body)"
fi

# Cache key index endpoint (/_index): replaces ListObjectsV2 with a binary
# blob (24 B header + sorted 32 B SHA-256 hashes + 32 B SHA-256 trailer).
KEY_A="go-buildcache/v1aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
KEY_B="go-buildcache/v1bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
KEY_C="go-buildcache/v1cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
for k in "$KEY_A" "$KEY_B" "$KEY_C"; do
  curl -sf -u "$AUTH" -X PUT --data-binary "data" \
    "$ENDPOINT_NORMAL/$BUCKET/$k" > /dev/null
done

INDEX_BODY=$(mktemp)
INDEX_HEADERS=$(mktemp)
HTTP_CODE=$(curl -s -o "$INDEX_BODY" -D "$INDEX_HEADERS" -w "%{http_code}" \
  -u "$AUTH" "$ENDPOINT_NORMAL/$BUCKET/_index")
INDEX_SIZE=$(stat -c %s "$INDEX_BODY")
EXPECTED_SIZE=$((24 + 3*32 + 32))
if [ "$HTTP_CODE" = "200" ] && [ "$INDEX_SIZE" = "$EXPECTED_SIZE" ]; then
  pass "/_index returns 200 with expected blob size"
else
  fail "/_index: code=$HTTP_CODE size=$INDEX_SIZE expected=$EXPECTED_SIZE"
fi

INDEX_MAGIC=$(head -c 4 "$INDEX_BODY")
if [ "$INDEX_MAGIC" = "GBCI" ]; then
  pass "/_index body has GBCI magic"
else
  fail "/_index body magic: got '$INDEX_MAGIC'"
fi

ETAG=$(grep -i '^etag:' "$INDEX_HEADERS" | tr -d '\r' | awk '{print $2}')
if [ -n "$ETAG" ]; then
  pass "/_index sets ETag header"
else
  fail "/_index missing ETag header"
fi

# If-None-Match with the matching ETag should return 304.
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -u "$AUTH" \
  -H "If-None-Match: $ETAG" "$ENDPOINT_NORMAL/$BUCKET/_index")
if [ "$HTTP_CODE" = "304" ]; then
  pass "/_index If-None-Match returns 304"
else
  fail "/_index If-None-Match: code=$HTTP_CODE expected 304"
fi

# Non-cacheprog keys must not appear in the index (size stays the same).
curl -sf -u "$AUTH" -X PUT --data-binary "x" \
  "$ENDPOINT_NORMAL/$BUCKET/notcacheprog/foo" > /dev/null
INDEX_SIZE2=$(curl -sf -u "$AUTH" -o /tmp/index2.bin -w "%{size_download}" \
  "$ENDPOINT_NORMAL/$BUCKET/_index")
if [ "$INDEX_SIZE2" = "$EXPECTED_SIZE" ]; then
  pass "/_index excludes non-cacheprog keys"
else
  fail "/_index after non-cacheprog PUT: size=$INDEX_SIZE2 expected=$EXPECTED_SIZE"
fi

# A new well-formed PUT bumps the index size and changes the ETag.
KEY_D="go-buildcache/v1dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
curl -sf -u "$AUTH" -X PUT --data-binary "data" \
  "$ENDPOINT_NORMAL/$BUCKET/$KEY_D" > /dev/null
INDEX_HEADERS2=$(mktemp)
HTTP_CODE=$(curl -s -o /tmp/index3.bin -D "$INDEX_HEADERS2" -w "%{http_code}" \
  -u "$AUTH" "$ENDPOINT_NORMAL/$BUCKET/_index")
INDEX_SIZE3=$(stat -c %s /tmp/index3.bin)
ETAG2=$(grep -i '^etag:' "$INDEX_HEADERS2" | tr -d '\r' | awk '{print $2}')
EXPECTED_SIZE2=$((24 + 4*32 + 32))
if [ "$HTTP_CODE" = "200" ] && [ "$INDEX_SIZE3" = "$EXPECTED_SIZE2" ] && [ "$ETAG2" != "$ETAG" ]; then
  pass "/_index reflects new PUT (size + ETag changed)"
else
  fail "/_index after new PUT: code=$HTTP_CODE size=$INDEX_SIZE3 expected=$EXPECTED_SIZE2 etag_changed=$([ "$ETAG2" != "$ETAG" ] && echo yes || echo no)"
fi

# Overwrite allowed in normal mode
curl -sf -u "$AUTH" -X PUT --data-binary "first" \
  "$ENDPOINT_NORMAL/$BUCKET/overwrite/v1test000000000002" > /dev/null
curl -sf -u "$AUTH" -X PUT --data-binary "second" \
  "$ENDPOINT_NORMAL/$BUCKET/overwrite/v1test000000000002" > /dev/null
OVERWRITE_BODY=$(curl -sf -u "$AUTH" \
  "$ENDPOINT_NORMAL/$BUCKET/overwrite/v1test000000000002")
if [ "$OVERWRITE_BODY" = "second" ]; then
  pass "Overwrite allowed in normal mode"
else
  fail "Overwrite allowed: expected 'second', got '$OVERWRITE_BODY'"
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

# Metadata roundtrip (native X-Cache-Meta-* headers).
curl -sf -u "$AUTH" -X PUT --data-binary "metabody" \
  -H "X-Cache-Meta-Outputid: abc123" -H "X-Cache-Meta-Custom: val2" \
  "$ENDPOINT_NORMAL/$BUCKET/metatest/v1meta000000000001" > /dev/null
META_OUTPUTID=$(curl -sf -u "$AUTH" -D /tmp/test-meta-headers -o /dev/null \
  "$ENDPOINT_NORMAL/$BUCKET/metatest/v1meta000000000001" \
  && grep -i 'x-cache-meta-outputid' /tmp/test-meta-headers | tr -d '\r' | awk '{print $2}')
if [ "$META_OUTPUTID" = "abc123" ]; then
  pass "Metadata roundtrip (native headers)"
else
  fail "Metadata roundtrip (native headers): outputid='$META_OUTPUTID'"
fi

# Deprecated S3 metadata path still works: a legacy client PUTs with
# X-Amz-Meta-* and the value is served back under both the native and legacy
# header names (so not-yet-upgraded clients keep getting cache hits).
curl -sf -u "$AUTH" -X PUT --data-binary "legacymeta" \
  -H "X-Amz-Meta-Outputid: legacy456" \
  "$ENDPOINT_NORMAL/$BUCKET/metatest/v1meta000000000002" > /dev/null
curl -sf -u "$AUTH" -D /tmp/test-meta-headers2 -o /dev/null \
  "$ENDPOINT_NORMAL/$BUCKET/metatest/v1meta000000000002"
LEGACY_NATIVE=$(grep -i 'x-cache-meta-outputid' /tmp/test-meta-headers2 | tr -d '\r' | awk '{print $2}')
LEGACY_AMZ=$(grep -i 'x-amz-meta-outputid' /tmp/test-meta-headers2 | tr -d '\r' | awk '{print $2}')
if [ "$LEGACY_NATIVE" = "legacy456" ] && [ "$LEGACY_AMZ" = "legacy456" ]; then
  pass "Deprecated X-Amz-Meta path still roundtrips (served under both names)"
else
  fail "Deprecated X-Amz-Meta path: native='$LEGACY_NATIVE' amz='$LEGACY_AMZ'"
fi

# ── Health Endpoint Tests ───────────────────────────────────────────────────

echo ""
echo "=== Health Endpoint Tests ==="

# /_health is unauthenticated (answered before the auth gate) and returns 200 "ok".
HEALTH_BODY=$(mktemp)
HTTP_CODE=$(curl -s -o "$HEALTH_BODY" -w "%{http_code}" "$ENDPOINT_NORMAL/_health")
if [ "$HTTP_CODE" = "200" ] && grep -q "ok" "$HEALTH_BODY"; then
  pass "/_health returns 200 without auth"
else
  fail "/_health: code=$HTTP_CODE body=$(cat "$HEALTH_BODY")"
fi

# ── Auth Tests ──────────────────────────────────────────────────────────────

echo ""
echo "=== Auth Tests ==="

# Wrong password
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
  -u "testuser:wrongpassword" \
  "$ENDPOINT_NORMAL/$BUCKET/auth/v1test000000000001")
if [ "$HTTP_CODE" = "403" ]; then
  pass "Wrong password returns 403"
else
  fail "Wrong password returned $HTTP_CODE, expected 403"
fi

# Unknown user
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
  -u "unknownuser:testpass" \
  "$ENDPOINT_NORMAL/$BUCKET/auth/v1test000000000002")
if [ "$HTTP_CODE" = "403" ]; then
  pass "Unknown user returns 403"
else
  fail "Unknown user returned $HTTP_CODE, expected 403"
fi

# No auth header
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
WO_BODY=$(curl -sf -u "$AUTH" \
  "$ENDPOINT_WRITEONCE/$BUCKET/wo/v1first000000000001")
if [ "$WO_BODY" = "first write" ]; then
  pass "Write-once: first PUT succeeds"
else
  fail "Write-once: first PUT content mismatch"
fi

# Same content is idempotent
curl -sf -u "$AUTH" -X PUT --data-binary "idempotent content" \
  "$ENDPOINT_WRITEONCE/$BUCKET/wo/v1idempotent0000001" > /dev/null
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -u "$AUTH" -X PUT \
  --data-binary "idempotent content" \
  "$ENDPOINT_WRITEONCE/$BUCKET/wo/v1idempotent0000001")
if [ "$HTTP_CODE" = "200" ]; then
  pass "Write-once: same content idempotent"
else
  fail "Write-once: same content PUT returned $HTTP_CODE, expected 200"
fi

# Different content returns 409
curl -sf -u "$AUTH" -X PUT --data-binary "original" \
  "$ENDPOINT_WRITEONCE/$BUCKET/wo/v1conflict000000001" > /dev/null
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -u "$AUTH" -X PUT \
  --data-binary "different" \
  "$ENDPOINT_WRITEONCE/$BUCKET/wo/v1conflict000000001")
if [ "$HTTP_CODE" = "409" ]; then
  pass "Write-once: different content returns 409"
else
  fail "Write-once: different content returned $HTTP_CODE, expected 409"
fi

# Original content preserved after conflict
WO_PRESERVED=$(curl -sf -u "$AUTH" \
  "$ENDPOINT_WRITEONCE/$BUCKET/wo/v1conflict000000001")
if [ "$WO_PRESERVED" = "original" ]; then
  pass "Write-once: original content preserved after conflict"
else
  fail "Write-once: expected 'original', got '$WO_PRESERVED'"
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
