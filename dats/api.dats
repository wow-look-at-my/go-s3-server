$schema: https://github.com/wow-look-at-my/dats/schema.json

shared:
	files:
		serve.sh: |
			# Runs a single check script against a server this starts and
			# stops. usage: serve.sh <config.json> <check.sh>
			# The config listens on port 0, so the kernel picks a free port and
			# the server log names it. Concurrent runs never share a server.
			# The check reads the port from $PORT.
			set -euo pipefail
			config="$1"
			check="$2"
			log="$(mktemp)"
			"${SERVER:-./build/go-s3-server}" --config "$config" > "$log" 2>&1 &
			server=$!
			trap 'kill "$server" 2>/dev/null || true; wait "$server" 2>/dev/null || true' EXIT
			port_re='listening on [^ ]*:([0-9]+) bucket='
			PORT=""
			ready=""
			for _ in $(seq 1 100); do
				while [ -z "$PORT" ] && IFS= read -r line; do
					if [[ "$line" =~ $port_re ]]; then PORT="${BASH_REMATCH[1]}"; fi
				done < "$log"
				if [ -n "$PORT" ] && curl -so /dev/null "http://127.0.0.1:$PORT/_health"; then ready=yes; break; fi
				if ! kill -0 "$server" 2>/dev/null; then break; fi
				sleep 0.1
			done
			export PORT
			if [ -z "$ready" ]; then
				echo "the server never answered on port ${PORT:-(none in its log)}" >&2
				sed 's/^/server: /' "$log" >&2
				exit 1
			fi
			# A failing check gets the server's log too. Without this a check
			# that cannot reach a server which HAD answered reports only its own
			# exit status. And the a single process that knows why says nothing.
			status=0
			bash "$check" || status=$?
			if [ "$status" -ne 0 ]; then
				echo "the check exited $status" >&2
				kill -0 "$server" 2>/dev/null || echo "the server had already exited" >&2
				sed 's/^/server: /' "$log" >&2
			fi
			exit "$status"

tests:
	- desc: a stored object comes back byte for byte
	  exit: 0
	  inputs:
		files:
			config.json: '{"listen":"127.0.0.1:0","bucket":"test-cache","dashboard_listen":"","data_dir":"{outputs.data}","credentials":[{"username":"testuser","password":"testpass"}]}'
			check.sh: |
				set -euo pipefail
				base=http://127.0.0.1:$PORT/test-cache
				curl -sf -u testuser:testpass -X PUT --data-binary 'hello world cache data' "$base/api/v1test000000000001" > /dev/null
				curl -sf -u testuser:testpass "$base/api/v1test000000000001"
	  cmd: bash {shared.serve.sh} {inputs.config.json} {inputs.check.sh}
	  outputs:
		stdout:
			0: "^hello world cache data$"

	- desc: a key that was never stored answers not_found in plain text, never S3 XML
	  exit: 0
	  inputs:
		files:
			config.json: '{"listen":"127.0.0.1:0","bucket":"test-cache","dashboard_listen":"","data_dir":"{outputs.data}","credentials":[{"username":"testuser","password":"testpass"}]}'
			check.sh: |
				set -euo pipefail
				base=http://127.0.0.1:$PORT/test-cache
				body="$(mktemp)"
				code=$(curl -s -o "$body" -w '%{http_code}' -u testuser:testpass "$base/nonexistent/v1xxxx000000000000")
				echo "status $code"
				sed 's/^/body /' "$body"
	  cmd: bash {shared.serve.sh} {inputs.config.json} {inputs.check.sh}
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
			config.json: '{"listen":"127.0.0.1:0","bucket":"test-cache","dashboard_listen":"","data_dir":"{outputs.data}","credentials":[{"username":"testuser","password":"testpass"}]}'
			check.sh: |
				set -euo pipefail
				base=http://127.0.0.1:$PORT/test-cache
				curl -sf -u testuser:testpass -X PUT --data-binary 'first' "$base/overwrite/v1test000000000002" > /dev/null
				curl -sf -u testuser:testpass -X PUT --data-binary 'second' "$base/overwrite/v1test000000000002" > /dev/null
				curl -sf -u testuser:testpass "$base/overwrite/v1test000000000002"
	  cmd: bash {shared.serve.sh} {inputs.config.json} {inputs.check.sh}
	  outputs:
		stdout:
			0: "^second$"

	- desc: a key lands on disk under its two-level shard
	  exit: 0
	  inputs:
		env:
			DATA_DIR: "{outputs.data}"
		files:
			config.json: '{"listen":"127.0.0.1:0","bucket":"test-cache","dashboard_listen":"","data_dir":"{outputs.data}","credentials":[{"username":"testuser","password":"testpass"}]}'
			check.sh: |
				set -euo pipefail
				base=http://127.0.0.1:$PORT/test-cache
				curl -sf -u testuser:testpass -X PUT --data-binary 'sharddata' "$base/shardtest/v1aabbccdd11223344" > /dev/null
				sed 's/^/shard /' "$DATA_DIR/shardtest/v1/aa/bbccdd11223344"
	  cmd: bash {shared.serve.sh} {inputs.config.json} {inputs.check.sh}
	  outputs:
		stdout:
			- "shard sharddata"

	- desc: metadata sent under the native header is served back under it
	  exit: 0
	  inputs:
		files:
			config.json: '{"listen":"127.0.0.1:0","bucket":"test-cache","dashboard_listen":"","data_dir":"{outputs.data}","credentials":[{"username":"testuser","password":"testpass"}]}'
			check.sh: |
				set -euo pipefail
				base=http://127.0.0.1:$PORT/test-cache
				curl -sf -u testuser:testpass -X PUT --data-binary 'metabody' \
					-H 'X-Cache-Meta-Outputid: abc123' -H 'X-Cache-Meta-Custom: val2' \
					"$base/metatest/v1meta000000000001" > /dev/null
				headers="$(mktemp)"
				curl -sf -u testuser:testpass -D "$headers" -o /dev/null "$base/metatest/v1meta000000000001"
				tr -d '\r' < "$headers" | grep -i '^x-cache-meta-'
	  cmd: bash {shared.serve.sh} {inputs.config.json} {inputs.check.sh}
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
			config.json: '{"listen":"127.0.0.1:0","bucket":"test-cache","dashboard_listen":"","data_dir":"{outputs.data}","credentials":[{"username":"testuser","password":"testpass"}]}'
			check.sh: |
				set -euo pipefail
				base=http://127.0.0.1:$PORT/test-cache
				curl -sf -u testuser:testpass -X PUT --data-binary 'legacymeta' \
					-H 'X-Amz-Meta-Outputid: legacy456' \
					"$base/metatest/v1meta000000000002" > /dev/null
				headers="$(mktemp)"
				curl -sf -u testuser:testpass -D "$headers" -o /dev/null "$base/metatest/v1meta000000000002"
				tr -d '\r' < "$headers" | grep -i 'meta-outputid'
	  cmd: bash {shared.serve.sh} {inputs.config.json} {inputs.check.sh}
	  outputs:
		stdout:
			- "X-Cache-Meta-Outputid: legacy456"
			- "X-Amz-Meta-Outputid: legacy456"

	- desc: the index advertises cacheprog keys only, and its ETag follows that set
	  exit: 0
	  inputs:
		files:
			config.json: '{"listen":"127.0.0.1:0","bucket":"test-cache","dashboard_listen":"","data_dir":"{outputs.data}","index_blob_interval":"0s","credentials":[{"username":"testuser","password":"testpass"}]}'
			check.sh: |
				set -euo pipefail
				base=http://127.0.0.1:$PORT/test-cache
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
	  cmd: bash {shared.serve.sh} {inputs.config.json} {inputs.check.sh}
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

	- desc: the index answers the short form to a client that asks with hash=8, with eight bytes per key, and the full form to one that does not
	  exit: 0
	  inputs:
		files:
			config.json: '{"listen":"127.0.0.1:19020","bucket":"test-cache","dashboard_listen":"","data_dir":"{outputs.data}","index_blob_interval":"0s","credentials":[{"username":"testuser","password":"testpass"}]}'
			check.sh: |
				set -euo pipefail
				base=http://127.0.0.1:19020/test-cache
				auth=testuser:testpass
				put() { curl -sf -u "$auth" -X PUT --data-binary 'data' "$base/go-buildcache/v1$1" > /dev/null; }
				header() { tr -d '\r' < "$1" | awk -v name="$2" 'tolower($1) == name {print $2}'; }
				for c in a b c; do put "$(printf "$c%.0s" $(seq 64))"; done

				v1="$(mktemp)"
				v1headers="$(mktemp)"
				curl -sf -u "$auth" -o "$v1" -D "$v1headers" "$base/_index"
				echo "v1 size $(stat -c %s "$v1")"
				echo "v1 header $(od -An -tu1 -j4 -N2 "$v1" | xargs)"

				v2="$(mktemp)"
				v2headers="$(mktemp)"
				curl -sf -u "$auth" -o "$v2" -D "$v2headers" "$base/_index?hash=8"
				echo "short size $(stat -c %s "$v2")"
				echo "short header $(od -An -tu1 -j4 -N2 "$v2" | xargs)"
				echo "short first $(od -An -tx1 -j24 -N8 "$v2" | tr -d ' ')"

				etag1=$(header "$v1headers" etag:)
				etag2=$(header "$v2headers" etag:)
				test -n "$etag2" && test "$etag2" != "$etag1" && echo "etags differ"
				echo "short revalidate $(curl -s -o /dev/null -w '%{http_code}' -u "$auth" -H "If-None-Match: $etag2" "$base/_index?hash=8")"
				echo "v1 etag on short $(curl -s -o /dev/null -w '%{http_code}' -u "$auth" -H "If-None-Match: $etag1" "$base/_index?hash=8")"

				epoch=$(header "$v2headers" x-index-epoch:)
				pos=$(header "$v2headers" x-index-seq:)
				put "$(printf 'd%.0s' $(seq 64))"
				delta="$(mktemp)"
				deltaheaders="$(mktemp)"
				curl -sf -u "$auth" -o "$delta" -D "$deltaheaders" "$base/_index?hash=8&epoch=$epoch&seq=$pos"
				echo "short delta $(header "$deltaheaders" x-index-delta:) size $(stat -c %s "$delta")"
				echo "short delta header $(od -An -tu1 -j4 -N2 "$delta" | xargs)"
				echo "short delta first $(od -An -tx1 -j24 -N8 "$delta" | tr -d ' ')"
	  cmd: bash {shared.serve.sh} {inputs.config.json} 19020 {inputs.check.sh}
	  outputs:
		stdout:
			- "v1 size 152"
			- "v1 header 1 32"
			- "short size 80"
			- "short header 1 8"
			- "short first aaaaaaaaaaaaaaaa"
			- "etags differ"
			- "short revalidate 304"
			- "v1 etag on short 200"
			- "short delta 1 size 64"
			- "short delta header 1 8"
			- "short delta first dddddddddddddddd"
	- desc: v=2 names the Rice-coded index form, with its own ETag
	  exit: 0
	  inputs:
		files:
			config.json: '{"listen":"127.0.0.1:0","bucket":"test-cache","dashboard_listen":"","data_dir":"{outputs.data}","index_blob_interval":"0s","credentials":[{"username":"testuser","password":"testpass"}]}'
			check.sh: |
				set -euo pipefail
				base=http://127.0.0.1:$PORT/test-cache
				auth=testuser:testpass
				put() { curl -sf -u "$auth" -X PUT --data-binary 'data' "$base/go-buildcache/v1$1" > /dev/null; }
				for c in a b c; do put "$(printf "$c%.0s" $(seq 64))"; done

				body="$(mktemp)"
				headers="$(mktemp)"
				code=$(curl -s -o "$body" -D "$headers" -w '%{http_code}' -u "$auth" "$base/_index?v=2")
				echo "status $code"
				echo "size $(stat -c %s "$body")"
				echo "magic $(dd if="$body" bs=1 count=4 2>/dev/null)"
				echo "version $(od -An -tu1 -j4 -N1 "$body" | tr -d ' ')"
				echo "bits $(od -An -tu1 -j5 -N1 "$body" | tr -d ' ')"
				etag=$(tr -d '\r' < "$headers" | awk 'tolower($1) == "etag:" {print $2}')
				test -n "$etag" && echo "etag present"

				code=$(curl -s -o /dev/null -w '%{http_code}' -u "$auth" -H "If-None-Match: $etag" "$base/_index?v=2")
				echo "revalidate $code"

				v1=$(curl -s -o /dev/null -D - -u "$auth" "$base/_index?hash=8" | tr -d '\r' | awk 'tolower($1) == "etag:" {print $2}')
				test "$v1" != "$etag" && echo "v1 etag differs"
				echo "v1-short-size $(curl -sf -u "$auth" -o /dev/null -w '%{size_download}' "$base/_index?hash=8")"

				put "$(printf 'd%.0s' $(seq 64))"
				headers2="$(mktemp)"
				echo "after-new-key $(curl -sf -u "$auth" -o /dev/null -D "$headers2" -w '%{size_download}' "$base/_index?v=2")"
				etag2=$(tr -d '\r' < "$headers2" | awk 'tolower($1) == "etag:" {print $2}')
				test "$etag2" != "$etag" && echo "etag moved"
	  cmd: bash {shared.serve.sh} {inputs.config.json} {inputs.check.sh}
	  outputs:
		stdout:
			- "status 200"
			- "size 71"
			- "magic GBCI"
			- "version 2"
			- "bits 40"
			- "etag present"
			- "revalidate 304"
			- "v1 etag differs"
			- "v1-short-size 80"
			- "after-new-key 76"
			- "etag moved"

	- desc: the health probe answers before the auth gate
	  exit: 0
	  inputs:
		files:
			config.json: '{"listen":"127.0.0.1:0","bucket":"test-cache","dashboard_listen":"","data_dir":"{outputs.data}","credentials":[{"username":"testuser","password":"testpass"}]}'
			check.sh: |
				set -euo pipefail
				body="$(mktemp)"
				code=$(curl -s -o "$body" -w '%{http_code}' http://127.0.0.1:$PORT/_health)
				echo "status $code"
				sed 's/^/body /' "$body"
	  cmd: bash {shared.serve.sh} {inputs.config.json} {inputs.check.sh}
	  outputs:
		stdout:
			- "status 200"
			- "body ok"

	- desc: the version probe answers before the auth gate with the build's own information
	  exit: 0
	  inputs:
		files:
			config.json: '{"listen":"127.0.0.1:0","bucket":"test-cache","dashboard_listen":"","data_dir":"{outputs.data}","credentials":[{"username":"testuser","password":"testpass"}]}'
			check.sh: |
				set -euo pipefail
				body="$(mktemp)"
				code=$(curl -s -o "$body" -w '%{http_code}' http://127.0.0.1:$PORT/_version)
				echo "status $code"
				echo "keys $(jq -r 'keys | join(",")' "$body")"
				echo "go $(jq -r '.go' "$body")"
	  cmd: bash {shared.serve.sh} {inputs.config.json} {inputs.check.sh}
	  outputs:
		stdout:
			- "status 200"
			- "keys go,modified,revision,time,version"
			- "go go1."

	- desc: a wrong password, an unknown user and no credentials at all are each refused
	  exit: 0
	  inputs:
		files:
			config.json: '{"listen":"127.0.0.1:0","bucket":"test-cache","dashboard_listen":"","data_dir":"{outputs.data}","credentials":[{"username":"testuser","password":"testpass"}]}'
			check.sh: |
				set -euo pipefail
				base=http://127.0.0.1:$PORT/test-cache
				echo "wrong-password $(curl -s -o /dev/null -w '%{http_code}' -u testuser:wrongpassword "$base/auth/v1test000000000001")"
				echo "unknown-user $(curl -s -o /dev/null -w '%{http_code}' -u unknownuser:testpass "$base/auth/v1test000000000002")"
				echo "no-credentials $(curl -s -o /dev/null -w '%{http_code}' "$base/auth/v1test000000000003")"
	  cmd: bash {shared.serve.sh} {inputs.config.json} {inputs.check.sh}
	  outputs:
		stdout:
			- "wrong-password 403"
			- "unknown-user 403"
			- "no-credentials 403"
