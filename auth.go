package main

import (
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"net/http"
	"strings"
)

// anonymousUser is the username recorded in audit logs when authentication has been explicitly disabled via disable_auth.
const anonymousUser = "<ANON>"

// authenticate verifies the request's credentials against the configured
// credentials. On success, it returns the authenticated username (or
// anonymousUser when disable_auth is set). On failure, it returns an error.
func authenticate(r *http.Request, cfg *Config) (string, error) {
	if cfg.DisableAuth {
		return anonymousUser, nil
	}

	auth := r.Header.Get("Authorization")
	if auth == "" {
		return "", fmt.Errorf("missing Authorization header")
	}

	if !strings.HasPrefix(auth, "Basic ") {
		return "", fmt.Errorf("unsupported auth scheme")
	}

	decoded, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(auth, "Basic "))
	if err != nil {
		return "", fmt.Errorf("invalid basic auth encoding")
	}

	parts := strings.SplitN(string(decoded), ":", 2)
	if len(parts) != 2 {
		return "", fmt.Errorf("invalid basic auth format")
	}
	if parts[0] == "" || parts[1] == "" {
		return "", fmt.Errorf("empty basic auth username or password")
	}

	for i := range cfg.Credentials {
		// Defense in depth: never match a credential with an empty username or
		// password.
		if cfg.Credentials[i].Username.Value == "" || cfg.Credentials[i].Password.Value == "" {
			continue
		}
		if subtle.ConstantTimeCompare([]byte(cfg.Credentials[i].Username.Value), []byte(parts[0])) == 1 &&
			subtle.ConstantTimeCompare([]byte(cfg.Credentials[i].Password.Value), []byte(parts[1])) == 1 {
			return cfg.Credentials[i].Username.Value, nil
		}
	}

	return "", fmt.Errorf("invalid credentials")
}
