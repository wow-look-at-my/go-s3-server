package main

import (
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"net/http"
	"strings"
)

func authenticate(r *http.Request, cfg *Config) error {
	for _, c := range cfg.Credentials {
		if c.Username == "" && c.Password == "" {
			return nil
		}
	}

	auth := r.Header.Get("Authorization")
	if auth == "" {
		return fmt.Errorf("missing Authorization header")
	}

	if !strings.HasPrefix(auth, "Basic ") {
		return fmt.Errorf("unsupported auth scheme")
	}

	decoded, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(auth, "Basic "))
	if err != nil {
		return fmt.Errorf("invalid basic auth encoding")
	}

	parts := strings.SplitN(string(decoded), ":", 2)
	if len(parts) != 2 {
		return fmt.Errorf("invalid basic auth format")
	}

	for i := range cfg.Credentials {
		if subtle.ConstantTimeCompare([]byte(cfg.Credentials[i].Username), []byte(parts[0])) == 1 &&
			subtle.ConstantTimeCompare([]byte(cfg.Credentials[i].Password), []byte(parts[1])) == 1 {
			return nil
		}
	}

	return fmt.Errorf("invalid credentials")
}
