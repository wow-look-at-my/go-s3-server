package cacheclient

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDetectCI pins the detection: CI carries a boolean, GITHUB_ACTIONS
// stands alone, and a false value is not a CI run.
func TestDetectCI(t *testing.T) {
	cases := []struct {
		name string
		env  map[string]string
		want bool
	}{
		{"empty environment", nil, false},
		{"CI=1", map[string]string{"CI": "1"}, true},
		{"CI=true", map[string]string{"CI": "true"}, true},
		{"CI=TRUE", map[string]string{"CI": "TRUE"}, true},
		{"CI=yes", map[string]string{"CI": "yes"}, true},
		{"CI=on", map[string]string{"CI": "on"}, true},
		{"CI padded", map[string]string{"CI": " true "}, true},
		{"CI=false", map[string]string{"CI": "false"}, false},
		{"CI=0", map[string]string{"CI": "0"}, false},
		{"CI empty", map[string]string{"CI": ""}, false},
		{"CI=maybe", map[string]string{"CI": "maybe"}, false},
		{"GITHUB_ACTIONS=true", map[string]string{"GITHUB_ACTIONS": "true"}, true},
		{"GITHUB_ACTIONS=false", map[string]string{"GITHUB_ACTIONS": "false"}, false},
		{"CI false but GITHUB_ACTIONS true", map[string]string{"CI": "false", "GITHUB_ACTIONS": "true"}, true},
		{"an unrelated variable", map[string]string{"CIRCLE": "true"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, detectCI(envFunc(tc.env)))
		})
	}
}

// TestRunningInCIReadsTheProcessEnvironment pins that the package's own entry
// point goes through the replaceable reader.
func TestRunningInCIReadsTheProcessEnvironment(t *testing.T) {
	stubCI(t, map[string]string{"CI": "true"})
	require.True(t, runningInCI())
	stubCI(t, nil)
	require.False(t, runningInCI())
}

// envFunc serves env as an os.Getenv-shaped lookup.
func envFunc(env map[string]string) func(string) string {
	return func(name string) string { return env[name] }
}

// stubCI points the CI detection at env for the rest of the test, so a run
// of this package under CI can still exercise the non-CI paths.
func stubCI(t *testing.T, env map[string]string) {
	t.Helper()
	prev := ciEnv
	ciEnv = envFunc(env)
	t.Cleanup(func() { ciEnv = prev })
}
