package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
)

type authInfo struct {
	AccessKey     string
	Date          string // YYYYMMDD
	Region        string
	Service       string
	SignedHeaders []string
	Signature     string // hex-encoded
}

func verifySignature(r *http.Request, cfg *Config) error {
	auth := r.Header.Get("Authorization")
	if auth == "" {
		return fmt.Errorf("missing Authorization header")
	}

	info, err := parseAuthHeader(auth)
	if err != nil {
		return fmt.Errorf("parse auth: %w", err)
	}

	cred := cfg.FindCredential(info.AccessKey)
	if cred == nil {
		return fmt.Errorf("unknown access key: %s", info.AccessKey)
	}

	// Build canonical request
	canonicalURI := uriEncodePath(r.URL.Path)
	canonicalQuery := canonicalQueryString(r.URL.RawQuery)
	canonicalHeaders, signedHeaderStr := buildCanonicalHeaders(r, info.SignedHeaders)

	payloadHash := r.Header.Get("X-Amz-Content-Sha256")
	if payloadHash == "" {
		payloadHash = "UNSIGNED-PAYLOAD"
	}

	canonicalRequest := strings.Join([]string{
		r.Method,
		canonicalURI,
		canonicalQuery,
		canonicalHeaders,
		signedHeaderStr,
		payloadHash,
	}, "\n")

	// String to sign
	amzDate := r.Header.Get("X-Amz-Date")
	scope := info.Date + "/" + info.Region + "/" + info.Service + "/aws4_request"
	canonHash := sha256.Sum256([]byte(canonicalRequest))
	stringToSign := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + hex.EncodeToString(canonHash[:])

	// Derive signing key and compute expected signature
	signingKey := deriveSigningKey(cred.SecretKey, info.Date, info.Region, info.Service)
	expectedSig := hmacSHA256(signingKey, []byte(stringToSign))
	expectedHex := hex.EncodeToString(expectedSig)

	// Constant-time comparison
	gotSig, err := hex.DecodeString(info.Signature)
	if err != nil {
		return fmt.Errorf("invalid signature hex")
	}
	if !hmac.Equal(expectedSig, gotSig) {
		return fmt.Errorf("signature mismatch: expected %s, got %s", expectedHex, info.Signature)
	}

	return nil
}

func parseAuthHeader(header string) (*authInfo, error) {
	// AWS4-HMAC-SHA256 Credential=AKID/20260328/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-..., Signature=abcdef
	if !strings.HasPrefix(header, "AWS4-HMAC-SHA256 ") {
		return nil, fmt.Errorf("unsupported auth scheme")
	}
	rest := strings.TrimPrefix(header, "AWS4-HMAC-SHA256 ")

	fields := make(map[string]string)
	for _, part := range strings.Split(rest, ", ") {
		idx := strings.Index(part, "=")
		if idx < 0 {
			return nil, fmt.Errorf("malformed auth field: %s", part)
		}
		fields[part[:idx]] = part[idx+1:]
	}

	credParts := strings.Split(fields["Credential"], "/")
	if len(credParts) != 5 {
		return nil, fmt.Errorf("malformed credential: %s", fields["Credential"])
	}

	signedHeaders := strings.Split(fields["SignedHeaders"], ";")
	sort.Strings(signedHeaders)

	return &authInfo{
		AccessKey:     credParts[0],
		Date:          credParts[1],
		Region:        credParts[2],
		Service:       credParts[3],
		SignedHeaders: signedHeaders,
		Signature:     fields["Signature"],
	}, nil
}

func buildCanonicalHeaders(r *http.Request, signedHeaders []string) (canonicalHeaders, signedHeaderStr string) {
	headerMap := make(map[string]string)
	// Use X-Forwarded-Host if present (set by reverse proxies), otherwise r.Host.
	// The client signs with the external endpoint, but behind a proxy r.Host is internal.
	host := r.Header.Get("X-Forwarded-Host")
	if host == "" {
		host = r.Host
	}
	headerMap["host"] = host
	for k, vals := range r.Header {
		headerMap[strings.ToLower(k)] = strings.TrimSpace(vals[0])
	}

	var canonical []string
	for _, h := range signedHeaders {
		canonical = append(canonical, h+":"+headerMap[h])
	}
	canonicalHeaders = strings.Join(canonical, "\n") + "\n"
	signedHeaderStr = strings.Join(signedHeaders, ";")
	return
}

func canonicalQueryString(raw string) string {
	if raw == "" {
		return ""
	}
	vals, err := url.ParseQuery(raw)
	if err != nil {
		return raw
	}
	return vals.Encode()
}

func uriEncodePath(path string) string {
	segments := strings.Split(path, "/")
	for i, seg := range segments {
		segments[i] = uriEncode(seg, true)
	}
	return strings.Join(segments, "/")
}

func uriEncode(s string, encodeSlash bool) string {
	var buf strings.Builder
	for _, b := range []byte(s) {
		if (b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z') ||
			(b >= '0' && b <= '9') || b == '-' || b == '_' || b == '.' || b == '~' {
			buf.WriteByte(b)
		} else if b == '/' && !encodeSlash {
			buf.WriteByte('/')
		} else {
			fmt.Fprintf(&buf, "%%%02X", b)
		}
	}
	return buf.String()
}

func deriveSigningKey(secret, datestamp, region, service string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secret), []byte(datestamp))
	kRegion := hmacSHA256(kDate, []byte(region))
	kService := hmacSHA256(kRegion, []byte(service))
	return hmacSHA256(kService, []byte("aws4_request"))
}

func hmacSHA256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}
