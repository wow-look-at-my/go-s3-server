$schema: https://github.com/wow-look-at-my/dats/schema.json

shared:
	files:
		serve.sh: |
			# Runs one check script against a server this starts, then prints the
			# server's own log so the test can assert on what it wrote.
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
			# The aggregator emits a second once it has ended, so give the
			# ticker a beat before reading the log.
			sleep 1.5
			sed 's/^/log /' "$log"

tests:
	- desc: normal mode reports one aggregated line per active second, and no per-request lines
	  exit: 0
	  inputs:
		files:
			config.json: '{"listen":"127.0.0.1:19050","dashboard_listen":"","bucket":"test-cache","data_dir":"{outputs.data}","credentials":[{"username":"testuser","password":"testpass"}]}'
			check.sh: |
				set -euo pipefail
				base=http://127.0.0.1:19050/test-cache
				auth=testuser:testpass
				# Two stores and two reads of a plain key, carrying the metadata a
				# real client sends: the module it built and the uncompressed size.
				curl -sf -u "$auth" -X PUT --data-binary '0123456789' \
					-H 'X-Cache-Meta-Module: example.com/alpha' \
					-H 'X-Cache-Meta-Body-Size: 40' \
					"$base/log/v1test000000000001" > /dev/null
				curl -sf -u "$auth" -X PUT --data-binary '0123456789' \
					-H 'X-Cache-Meta-Module: example.com/beta' \
					-H 'X-Cache-Meta-Body-Size: 40' \
					"$base/log/v1test000000000002" > /dev/null
				curl -sf -u "$auth" "$base/log/v1test000000000001" > /dev/null
				curl -sf -u "$auth" "$base/log/v1test000000000002" > /dev/null
	  cmd: bash {shared.serve.sh} {inputs.config.json} 19050 {inputs.check.sh}
	  outputs:
		stdout:
			- "access log: normal mode"
			- "cache 1s: put=2 get=2"
			- "ratio=25%"
			- "projects=example.com/alpha, example.com/beta"
		"!stdout":
			# The per-request line belongs to verbose mode only.
			- "req method=PUT"
			- "req method=GET"

	- desc: verbose mode reports each request once, with the handler detail on that same line
	  exit: 0
	  inputs:
		files:
			config.json: '{"listen":"127.0.0.1:19051","dashboard_listen":"","log_mode":"verbose","bucket":"test-cache","data_dir":"{outputs.data}","credentials":[{"username":"testuser","password":"testpass"}]}'
			check.sh: |
				set -euo pipefail
				base=http://127.0.0.1:19051/test-cache
				auth=testuser:testpass
				curl -sf -u "$auth" -X PUT --data-binary 'verbose body' "$base/log/v1verbose00000001" > /dev/null
				curl -sf -u "$auth" -X POST -H 'Content-Type: application/json' \
					--data '{"keys":["log/v1verbose00000001"],"prefetch":true}' \
					"$base/_batch/get" > /dev/null
	  cmd: bash {shared.serve.sh} {inputs.config.json} 19051 {inputs.check.sh}
	  outputs:
		stdout:
			- "access log: verbose mode"
			- "req method=PUT path=/test-cache/log/v1verbose00000001"
			- "batch_get requested=1"
		"!stdout":
			# The batch summary rides the request line. A second line about the
			# same request is the duplication this mode had.
			- "batch get: requested"
			- "cache 1s:"

	- desc: a silent second produces no line at all
	  exit: 0
	  inputs:
		files:
			config.json: '{"listen":"127.0.0.1:19052","dashboard_listen":"","bucket":"test-cache","data_dir":"{outputs.data}","credentials":[{"username":"testuser","password":"testpass"}]}'
			check.sh: |
				set -euo pipefail
				# No cache traffic at all: only the health probe serve.sh already
				# made, which is not an object move.
				sleep 2
	  cmd: bash {shared.serve.sh} {inputs.config.json} 19052 {inputs.check.sh}
	  outputs:
		"!stdout":
			- "cache 1s:"
