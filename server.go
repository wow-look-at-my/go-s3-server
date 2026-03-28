package main

import (
	"log"
	"net/http"
	"strings"
)

type Server struct {
	config  *Config
	storage *Storage
}

func NewServer(cfg *Config, storage *Storage) *Server {
	return &Server{config: cfg, storage: storage}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if err := verifySignature(r, s.config); err != nil {
		log.Printf("auth: %v", err)
		writeS3Error(w, 403, "AccessDenied", "Access Denied")
		return
	}

	// Parse path: /{bucket}/{key...} or /{bucket}
	path := strings.TrimPrefix(r.URL.Path, "/")
	parts := strings.SplitN(path, "/", 2)
	bucket := parts[0]

	if bucket != s.config.Bucket {
		writeS3Error(w, 404, "NoSuchBucket", "The specified bucket does not exist")
		return
	}

	key := ""
	if len(parts) == 2 {
		key = parts[1]
	}

	switch {
	case r.Method == "GET" && key == "" && r.URL.Query().Get("list-type") == "2":
		handleListObjectsV2(w, r, s.storage, bucket)
	case r.Method == "GET" && key != "":
		handleGetObject(w, r, s.storage, key)
	case r.Method == "PUT" && key != "":
		handlePutObject(w, r, s.storage, key)
	default:
		writeS3Error(w, 405, "MethodNotAllowed", "Method not allowed")
	}
}
