$schema: https://github.com/wow-look-at-my/dats/schema.json

shared:
	files:
		serve.sh: |
			# Runs a single check script against a server this starts and
			# stops. usage: serve.sh <config.json> <port> <check.sh>
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
	- desc: the first PUT of a key stores it
	  exit: 0
	  inputs:
		files:
			config.json: '{"listen":"127.0.0.1:19060","bucket":"test-cache","dashboard_listen":"","data_dir":"{outputs.data}","write_once":{"action":"deny","notification":"content_differs"},"credentials":[{"username":"testuser","password":"testpass"}]}'
			check.sh: |
				set -euo pipefail
				base=http://127.0.0.1:19060/test-cache
				curl -sf -u testuser:testpass -X PUT --data-binary 'first write' "$base/wo/v1first000000000001" > /dev/null
				curl -sf -u testuser:testpass "$base/wo/v1first000000000001"
	  cmd: bash {shared.serve.sh} {inputs.config.json} 19060 {inputs.check.sh}
	  outputs:
		stdout:
			0: "^first write$"

	- desc: re-sending the same bytes is accepted, so a racing uploader is not an error
	  exit: 0
	  inputs:
		files:
			config.json: '{"listen":"127.0.0.1:19061","bucket":"test-cache","dashboard_listen":"","data_dir":"{outputs.data}","write_once":{"action":"deny","notification":"content_differs"},"credentials":[{"username":"testuser","password":"testpass"}]}'
			check.sh: |
				set -euo pipefail
				base=http://127.0.0.1:19061/test-cache
				curl -sf -u testuser:testpass -X PUT --data-binary 'idempotent content' "$base/wo/v1idempotent0000001" > /dev/null
				echo "resend $(curl -s -o /dev/null -w '%{http_code}' -u testuser:testpass -X PUT --data-binary 'idempotent content' "$base/wo/v1idempotent0000001")"
	  cmd: bash {shared.serve.sh} {inputs.config.json} 19061 {inputs.check.sh}
	  outputs:
		stdout:
			- "resend 200"

	- desc: different bytes under a stored key are refused and the stored body survives
	  exit: 0
	  inputs:
		files:
			config.json: '{"listen":"127.0.0.1:19062","bucket":"test-cache","dashboard_listen":"","data_dir":"{outputs.data}","write_once":{"action":"deny","notification":"content_differs"},"credentials":[{"username":"testuser","password":"testpass"}]}'
			check.sh: |
				set -euo pipefail
				base=http://127.0.0.1:19062/test-cache
				curl -sf -u testuser:testpass -X PUT --data-binary 'original' "$base/wo/v1conflict000000001" > /dev/null
				echo "conflict $(curl -s -o /dev/null -w '%{http_code}' -u testuser:testpass -X PUT --data-binary 'different' "$base/wo/v1conflict000000001")"
				echo "stored $(curl -sf -u testuser:testpass "$base/wo/v1conflict000000001")"
	  cmd: bash {shared.serve.sh} {inputs.config.json} 19062 {inputs.check.sh}
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
			config.json: '{"listen":"127.0.0.1:19063","bucket":"test-cache","dashboard_listen":"","data_dir":"{outputs.data}","write_once":{"action":"deny","notification":"content_differs"},"credentials":[{"username":"testuser","password":"testpass"}]}'
			check.sh: |
				set -euo pipefail
				base=http://127.0.0.1:19063/test-cache
				curl -sf -u testuser:testpass -X PUT --data-binary 'wosharddata' "$base/woshard/v1aabbccdd99887766" > /dev/null
				sed 's/^/shard /' "$DATA_DIR/woshard/v1/aa/bbccdd99887766"
	  cmd: bash {shared.serve.sh} {inputs.config.json} 19063 {inputs.check.sh}
	  outputs:
		stdout:
			- "shard wosharddata"
