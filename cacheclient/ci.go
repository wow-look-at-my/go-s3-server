// ciEnv reads an environment variable for the CI detection.

package cacheclient

import (
	"os"
	"strings"
)

// ciEnv reads an environment variable for the CI detection.
var ciEnv = os.Getenv

// ciEnvVars are the variables whose true-ish value marks a CI run.
var ciEnvVars = []string{"CI", "GITHUB_ACTIONS"}

// runningInCI reports whether this process belongs to a CI run.
func runningInCI() bool {
	return detectCI(ciEnv)
}

// detectCI reports whether the environment getenv reads marks a CI run.
func detectCI(getenv func(string) string) bool {
	for _, name := range ciEnvVars {
		if envIsTrue(getenv(name)) {
			return true
		}
	}
	return false
}

// envIsTrue reports whether an environment value spells a true boolean.
func envIsTrue(v string) bool {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}
