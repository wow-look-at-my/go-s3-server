$schema: https://github.com/wow-look-at-my/dats/schema.json

tests:
	- desc: servers started at once from port-0 configs bind distinct ports and each answers its own health probe
	  exit: 0
	  inputs:
		files:
			a.json: '{"listen":"127.0.0.1:0","dashboard_listen":"127.0.0.1:0","bucket":"test-cache","data_dir":"{outputs.a}","credentials":[{"username":"testuser","password":"testpass"}]}'
			b.json: '{"listen":"127.0.0.1:0","dashboard_listen":"127.0.0.1:0","bucket":"test-cache","data_dir":"{outputs.b}","credentials":[{"username":"testuser","password":"testpass"}]}'
			check.sh: |
				set -euo pipefail
				server_bin="${SERVER:-./build/go-s3-server}"
				log_a="$(mktemp)"
				log_b="$(mktemp)"
				"$server_bin" --config "$1" > "$log_a" 2>&1 &
				pid_a=$!
				"$server_bin" --config "$2" > "$log_b" 2>&1 &
				pid_b=$!
				trap 'kill "$pid_a" "$pid_b" 2>/dev/null || true; wait 2>/dev/null || true' EXIT
				port_re='listening on [^ ]*:([0-9]+) bucket='
				dash_re='dashboard listening on [^ ]*:([0-9]+);'
				# port_of prints the port the line that matches $2 names in log $1.
				port_of() {
					local found=""
					for _ in $(seq 1 100); do
						while IFS= read -r line; do
							if [[ "$line" =~ $2 ]]; then found="${BASH_REMATCH[1]}"; fi
						done < "$1"
						if [ -n "$found" ]; then echo "$found"; return 0; fi
						sleep 0.1
					done
					echo "no port in $1" >&2
					while IFS= read -r line; do echo "server: $line" >&2; done < "$1"
					return 1
				}
				port_a="$(port_of "$log_a" "$port_re")"
				port_b="$(port_of "$log_b" "$port_re")"
				dash_a="$(port_of "$log_a" "$dash_re")"
				dash_b="$(port_of "$log_b" "$dash_re")"
				test "$port_a" != "$port_b" && echo "cache ports differ"
				test "$dash_a" != "$dash_b" && echo "dashboard ports differ"
				echo "a health $(curl -sf "http://127.0.0.1:$port_a/_health")"
				echo "b health $(curl -sf "http://127.0.0.1:$port_b/_health")"
				# A key stored through one server is absent from the other, so
				# each port reaches its own data.
				curl -sf -u testuser:testpass -X PUT --data-binary 'only in a' "http://127.0.0.1:$port_a/test-cache/ports/v1test000000000001" > /dev/null
				echo "b sees a $(curl -s -o /dev/null -w '%{http_code}' -u testuser:testpass "http://127.0.0.1:$port_b/test-cache/ports/v1test000000000001")"
				echo "a dashboard $(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$dash_a/")"
				echo "b dashboard $(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$dash_b/")"
	  cmd: bash {inputs.check.sh} {inputs.a.json} {inputs.b.json}
	  outputs:
		stdout:
			- "cache ports differ"
			- "dashboard ports differ"
			- "a health ok"
			- "b health ok"
			- "b sees a 404"
			- "a dashboard 200"
			- "b dashboard 200"

	- desc: no suite names a fixed port, so concurrent runs of a suite never share a server
	  exit: 0
	  cmd: bash -c 'echo "fixed-ports $(grep -cE "\"(dashboard_|metrics_)?listen\" *: *\"[^\"]*:[1-9]|127\.0\.0\.1:[1-9]|localhost:[1-9]" dats/*.dats | grep -vc ":0$")"; grep -nE "\"(dashboard_|metrics_)?listen\" *: *\"[^\"]*:[1-9]|127\.0\.0\.1:[1-9]|localhost:[1-9]" dats/*.dats || true'
	  outputs:
		stdout:
			- "fixed-ports 0"
