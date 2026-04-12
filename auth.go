package main

import (
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"net/http"
	"strings"
)

func verifySignature(r *http.Request, cfg *Config) error {
	auth := r.Header.Get("Authorization")
	if auth == "" {
		return fmt.Errorf("missing Authorization header")
	}

	if !strings.HasPrefix(auth, "Basic ") {
		return fmt.Errorf("unsupported auth scheme")
	}

	decoded, err := base64.StdEncoding.DecodeString(auth[len("Basic "):])
	if err != nil {
		return fmt.Errorf("invalid base64 in Authorization header")
	}

	parts := strings.SplitN(string(decoded), ":", 2)
	if len(parts) != 2 {
		return fmt.Errorf("malformed Basic auth value")
	}
	username, password := parts[0], parts[1]

	cred := cfg.FindCredential(username)
	if cred == nil {
		return fmt.Errorf("unknown access key: %s", username)
	}

	if subtle.ConstantTimeCompare([]byte(password), []byte(cred.SecretKey)) != 1 {
		return fmt.Errorf("invalid secret key for access key: %s", username)
	}

	return nil
}
