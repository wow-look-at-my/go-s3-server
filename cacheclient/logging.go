// Logger receives the client's diagnostics.

package cacheclient

// Logger receives the client's diagnostics.
type Logger interface {
	Infof(format string, args ...any)
	Warnf(format string, args ...any)
	Debugf(format string, args ...any)
}

// logging is where the client's diagnostics go. SetLogger replaces it.
var logging Logger = discardLogger{}

// SetLogger installs the destination for the client's diagnostics. Call it
// before the earliest Client, and pass nil to go back to silence.
func SetLogger(l Logger) {
	if l == nil {
		l = discardLogger{}
	}
	logging = l
}

// discardLogger drops every message, so no call site needs a nil check.
type discardLogger struct{}

func (discardLogger) Infof(string, ...any)  {}
func (discardLogger) Warnf(string, ...any)  {}
func (discardLogger) Debugf(string, ...any) {}
