package cacheclient

import (
	"os"
	"strings"
)

// ciEnv reads an environment variable for the CI detection. A test replaces
// it, so a run of this package's own tests under CI can still exercise the
// non-CI behaviour.
var ciEnv = os.Getenv

// ciEnvVars are the variables whose true-ish value marks a CI run. CI is the
// de facto standard, set by GitHub Actions, GitLab CI, CircleCI, Travis,
// Buildkite and Jenkins. GITHUB_ACTIONS is the workflow-specific marker and
// stands alone, because a job can clear CI.
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

// envIsTrue reports whether an environment value spells a true boolean. An
// unset, empty or false value reads as false, so CI=false is not a CI run.
func envIsTrue(v string) bool {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}
