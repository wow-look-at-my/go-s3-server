$schema: https://github.com/wow-look-at-my/dats/schema.json

shared:
	files:
		serve.sh: |
			# Runs one check script against a server this starts and stops.
			# usage: serve.sh <config.json> <port> <check.sh>
			set -euo pipefail
			config="$1"
			port="$2"
			check="$3"
			log="$(mktemp)"
			"${SERVER:-./build/go-s3-server}" --config "$config" > "$log" 2>&1 &
			server=$!
			trap 'kill "$server" 2>/dev/null || true; wait "$server" 2>/dev/null || true' EXIT
			ready=""
			for _ in $(seq 1 100); do
				if curl -so /dev/null "http://127.0.0.1:$port/_health"; then ready=yes; break; fi
				if ! kill -0 "$server" 2>/dev/null; then break; fi
				sleep 0.1
			done
			if [ -z "$ready" ]; then
				echo "the server never answered on port $port" >&2
				sed 's/^/server: /' "$log" >&2
				exit 1
			fi
			bash "$check"

tests:
	- desc: a stored object comes back byte for byte
	  exit: 0
	  inputs:
		files:
			config.json: '{"listen":"127.0.0.1:19010","bucket":"test-cache","data_dir":"{outputs.data}","credentials":[{"username":"testuser","password":"testpass"}]}'
			check.sh: |
				set -euo pipefail
				base=http://127.0.0.1:19010/test-cache
				curl -sf -u testuser:testpass -X PUT --data-binary 'hello world cache data' "$base/api/v1test000000000001" > /dev/null
				curl -sf -u testuser:testpass "$base/api/v1test000000000001"
	  cmd: bash {shared.serve.sh} {inputs.config.json} 19010 {inputs.check.sh}
	  outputs:
		stdout:
			0: "^hello world cache data$"

	- desc: a key that was never stored answers not_found in plain text, never S3 XML
	  exit: 0
	  inputs:
		files:
			config.json: '{"listen":"127.0.0.1:19011","bucket":"test-cache","data_dir":"{outputs.data}","credentials":[{"username":"testuser","password":"testpass"}]}'
			check.sh: |
				set -euo pipefail
				base=http://127.0.0.1:19011/test-cache
				body="$(mktemp)"
				code=$(curl -s -o "$body" -w '%{http_code}' -u testuser:testpass "$base/nonexistent/v1xxxx000000000000")
				echo "status $code"
				sed 's/^/body /' "$body"
	  cmd: bash {shared.serve.sh} {inputs.config.json} 19011 {inputs.check.sh}
	  outputs:
		stdout:
			- "status 404"
			- "body not_found"
		"!stdout":
			- "<Error>"

	- desc: a second PUT of different bytes replaces the first in normal mode
	  exit: 0
	  inputs:
		files:
			config.json: '{"listen":"127.0.0.1:19012","bucket":"test-cache","data_dir":"{outputs.data}","credentials":[{"username":"testuser","password":"testpass"}]}'
			check.sh: |
				set -euo pipefail
				base=http://127.0.0.1:19012/test-cache
				curl -sf -u testuser:testpass -X PUT --data-binary 'first' "$base/overwrite/v1test000000000002" > /dev/null
				curl -sf -u testuser:testpass -X PUT --data-binary 'second' "$base/overwrite/v1test000000000002" > /dev/null
				curl -sf -u testuser:testpass "$base/overwrite/v1test000000000002"
	  cmd: bash {shared.serve.sh} {inputs.config.json} 19012 {inputs.check.sh}
	  outputs:
		stdout:
			0: "^second$"

	- desc: a key lands on disk under its two-level shard
	  exit: 0
	  inputs:
		env:
			DATA_DIR: "{outputs.data}"
		files:
			config.json: '{"listen":"127.0.0.1:19013","bucket":"test-cache","data_dir":"{outputs.data}","credentials":[{"username":"testuser","password":"testpass"}]}'
			check.sh: |
				set -euo pipefail
				base=http://127.0.0.1:19013/test-cache
				curl -sf -u testuser:testpass -X PUT --data-binary 'sharddata' "$base/shardtest/v1aabbccdd11223344" > /dev/null
				sed 's/^/shard /' "$DATA_DIR/shardtest/v1/aa/bbccdd11223344"
	  cmd: bash {shared.serve.sh} {inputs.config.json} 19013 {inputs.check.sh}
	  outputs:
		stdout:
			- "shard sharddata"

	- desc: metadata sent under the native header is served back under it
	  exit: 0
	  inputs:
		files:
			config.json: '{"listen":"127.0.0.1:19014","bucket":"test-cache","data_dir":"{outputs.data}","credentials":[{"username":"testuser","password":"testpass"}]}'
			check.sh: |
				set -euo pipefail
				base=http://127.0.0.1:19014/test-cache
				curl -sf -u testuser:testpass -X PUT --data-binary 'metabody' \
					-H 'X-Cache-Meta-Outputid: abc123' -H 'X-Cache-Meta-Custom: val2' \
					"$base/metatest/v1meta000000000001" > /dev/null
				headers="$(mktemp)"
				curl -sf -u testuser:testpass -D "$headers" -o /dev/null "$base/metatest/v1meta000000000001"
				tr -d '\r' < "$headers" | grep -i '^x-cache-meta-'
	  cmd: bash {shared.serve.sh} {inputs.config.json} 19014 {inputs.check.sh}
	  outputs:
		stdout:
			- "Outputid: abc123"
			- "Custom: val2"
		"!stdout":
			- "Audit"

	- desc: a legacy X-Amz-Meta upload is served under both the native and the legacy name
	  exit: 0
	  inputs:
		files:
			config.json: '{"listen":"127.0.0.1:19015","bucket":"test-cache","data_dir":"{outputs.data}","credentials":[{"username":"testuser","password":"testpass"}]}'
			check.sh: |
				set -euo pipefail
				base=http://127.0.0.1:19015/test-cache
				curl -sf -u testuser:testpass -X PUT --data-binary 'legacymeta' \
					-H 'X-Amz-Meta-Outputid: legacy456' \
					"$base/metatest/v1meta000000000002" > /dev/null
				headers="$(mktemp)"
				curl -sf -u testuser:testpass -D "$headers" -o /dev/null "$base/metatest/v1meta000000000002"
				tr -d '\r' < "$headers" | grep -i 'meta-outputid'
	  cmd: bash {shared.serve.sh} {inputs.config.json} 19015 {inputs.check.sh}
	  outputs:
		stdout:
			- "X-Cache-Meta-Outputid: legacy456"
			- "X-Amz-Meta-Outputid: legacy456"

	- desc: the index advertises cacheprog keys only, and its ETag follows that set
	  exit: 0
	  inputs:
		files:
			config.json: '{"listen":"127.0.0.1:19016","bucket":"test-cache","data_dir":"{outputs.data}","credentials":[{"username":"testuser","password":"testpass"}]}'
			check.sh: |
				set -euo pipefail
				base=http://127.0.0.1:19016/test-cache
				auth=testuser:testpass
				put() { curl -sf -u "$auth" -X PUT --data-binary 'data' "$base/go-buildcache/v1$1" > /dev/null; }
				for c in a b c; do put "$(printf "$c%.0s" $(seq 64))"; done

				body="$(mktemp)"
				headers="$(mktemp)"
				code=$(curl -s -o "$body" -D "$headers" -w '%{http_code}' -u "$auth" "$base/_index")
				echo "status $code"
				echo "size $(stat -c %s "$body")"
				echo "magic $(dd if="$body" bs=1 count=4 2>/dev/null)"
				etag=$(tr -d '\r' < "$headers" | awk 'tolower($1) == "etag:" {print $2}')
				test -n "$etag" && echo "etag present"

				code=$(curl -s -o /dev/null -w '%{http_code}' -u "$auth" -H "If-None-Match: $etag" "$base/_index")
				echo "revalidate $code"

				# A key outside the cacheprog grammar carries no action hash, so it is storable but not advertisable.
				curl -sf -u "$auth" -X PUT --data-binary 'x' "$base/notcacheprog/foo" > /dev/null
				echo "after-other-key $(curl -sf -u "$auth" -o /dev/null -w '%{size_download}' "$base/_index")"

				put "$(printf 'd%.0s' $(seq 64))"
				body2="$(mktemp)"
				headers2="$(mktemp)"
				curl -sf -u "$auth" -o "$body2" -D "$headers2" "$base/_index"
				echo "after-new-key $(stat -c %s "$body2")"
				etag2=$(tr -d '\r' < "$headers2" | awk 'tolower($1) == "etag:" {print $2}')
				test "$etag2" != "$etag" && echo "etag moved"
	  cmd: bash {shared.serve.sh} {inputs.config.json} 19016 {inputs.check.sh}
	  outputs:
		stdout:
			- "status 200"
			- "size 152"
			- "magic GBCI"
			- "etag present"
			- "revalidate 304"
			- "after-other-key 152"
			- "after-new-key 184"
			- "etag moved"

	- desc: the health probe answers before the auth gate
	  exit: 0
	  inputs:
		files:
			config.json: '{"listen":"127.0.0.1:19017","bucket":"test-cache","data_dir":"{outputs.data}","credentials":[{"username":"testuser","password":"testpass"}]}'
			check.sh: |
				set -euo pipefail
				body="$(mktemp)"
				code=$(curl -s -o "$body" -w '%{http_code}' http://127.0.0.1:19017/_health)
				echo "status $code"
				sed 's/^/body /' "$body"
	  cmd: bash {shared.serve.sh} {inputs.config.json} 19017 {inputs.check.sh}
	  outputs:
		stdout:
			- "status 200"
			- "body ok"

	- desc: a wrong password, an unknown user and no credentials at all are each refused
	  exit: 0
	  inputs:
		files:
			config.json: '{"listen":"127.0.0.1:19018","bucket":"test-cache","data_dir":"{outputs.data}","credentials":[{"username":"testuser","password":"testpass"}]}'
			check.sh: |
				set -euo pipefail
				base=http://127.0.0.1:19018/test-cache
				echo "wrong-password $(curl -s -o /dev/null -w '%{http_code}' -u testuser:wrongpassword "$base/auth/v1test000000000001")"
				echo "unknown-user $(curl -s -o /dev/null -w '%{http_code}' -u unknownuser:testpass "$base/auth/v1test000000000002")"
				echo "no-credentials $(curl -s -o /dev/null -w '%{http_code}' "$base/auth/v1test000000000003")"
	  cmd: bash {shared.serve.sh} {inputs.config.json} 19018 {inputs.check.sh}
	  outputs:
		stdout:
			- "wrong-password 403"
			- "unknown-user 403"
			- "no-credentials 403"
