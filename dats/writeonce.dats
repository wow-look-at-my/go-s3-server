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
	- desc: the first PUT of a key stores it
	  exit: 0
	  inputs:
		files:
			config.json: '{"listen":"127.0.0.1:19030","bucket":"test-cache","data_dir":"{outputs.data}","write_once":{"action":"deny","notification":"content_differs"},"credentials":[{"username":"testuser","password":"testpass"}]}'
			check.sh: |
				set -euo pipefail
				base=http://127.0.0.1:19030/test-cache
				curl -sf -u testuser:testpass -X PUT --data-binary 'first write' "$base/wo/v1first000000000001" > /dev/null
				curl -sf -u testuser:testpass "$base/wo/v1first000000000001"
	  cmd: bash {shared.serve.sh} {inputs.config.json} 19030 {inputs.check.sh}
	  outputs:
		stdout:
			0: "^first write$"

	- desc: re-sending the same bytes is accepted, so a racing uploader is not an error
	  exit: 0
	  inputs:
		files:
			config.json: '{"listen":"127.0.0.1:19031","bucket":"test-cache","data_dir":"{outputs.data}","write_once":{"action":"deny","notification":"content_differs"},"credentials":[{"username":"testuser","password":"testpass"}]}'
			check.sh: |
				set -euo pipefail
				base=http://127.0.0.1:19031/test-cache
				curl -sf -u testuser:testpass -X PUT --data-binary 'idempotent content' "$base/wo/v1idempotent0000001" > /dev/null
				echo "resend $(curl -s -o /dev/null -w '%{http_code}' -u testuser:testpass -X PUT --data-binary 'idempotent content' "$base/wo/v1idempotent0000001")"
	  cmd: bash {shared.serve.sh} {inputs.config.json} 19031 {inputs.check.sh}
	  outputs:
		stdout:
			- "resend 200"

	- desc: different bytes under a stored key are refused and the stored body survives
	  exit: 0
	  inputs:
		files:
			config.json: '{"listen":"127.0.0.1:19032","bucket":"test-cache","data_dir":"{outputs.data}","write_once":{"action":"deny","notification":"content_differs"},"credentials":[{"username":"testuser","password":"testpass"}]}'
			check.sh: |
				set -euo pipefail
				base=http://127.0.0.1:19032/test-cache
				curl -sf -u testuser:testpass -X PUT --data-binary 'original' "$base/wo/v1conflict000000001" > /dev/null
				echo "conflict $(curl -s -o /dev/null -w '%{http_code}' -u testuser:testpass -X PUT --data-binary 'different' "$base/wo/v1conflict000000001")"
				echo "stored $(curl -sf -u testuser:testpass "$base/wo/v1conflict000000001")"
	  cmd: bash {shared.serve.sh} {inputs.config.json} 19032 {inputs.check.sh}
	  outputs:
		stdout:
			- "conflict 409"
			- "stored original"

	- desc: write-once mode shards a key the same way normal mode does
	  exit: 0
	  inputs:
		env:
			DATA_DIR: "{outputs.data}"
		files:
			config.json: '{"listen":"127.0.0.1:19033","bucket":"test-cache","data_dir":"{outputs.data}","write_once":{"action":"deny","notification":"content_differs"},"credentials":[{"username":"testuser","password":"testpass"}]}'
			check.sh: |
				set -euo pipefail
				base=http://127.0.0.1:19033/test-cache
				curl -sf -u testuser:testpass -X PUT --data-binary 'wosharddata' "$base/woshard/v1aabbccdd99887766" > /dev/null
				sed 's/^/shard /' "$DATA_DIR/woshard/v1/aa/bbccdd99887766"
	  cmd: bash {shared.serve.sh} {inputs.config.json} 19033 {inputs.check.sh}
	  outputs:
		stdout:
			- "shard wosharddata"
