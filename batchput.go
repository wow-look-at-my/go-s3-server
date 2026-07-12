package main

import (
	"archive/tar"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
)

type batchPutManifest struct {
	Entries []batchPutManifestEntry `json:"entries"`
}

type batchPutManifestEntry struct {
	Key      string            `json:"key"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

type batchPutResponse struct {
	Results []batchPutResult `json:"results"`
}

type batchPutResult struct {
	Key     string `json:"key"`
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

func handleBatchPut(w http.ResponseWriter, r *http.Request, storage *Storage) {
	tr := tar.NewReader(io.LimitReader(r.Body, 256<<20))

	var manifest batchPutManifest
	hdr, err := tr.Next()
	if err != nil {
		writeS3Error(w, 400, "InvalidRequest", fmt.Sprintf("read tar: %v", err))
		return
	}
	if hdr.Name != "manifest.json" {
		writeS3Error(w, 400, "InvalidRequest", "first tar entry must be manifest.json")
		return
	}
	if err := json.NewDecoder(tr).Decode(&manifest); err != nil {
		writeS3Error(w, 400, "InvalidRequest", fmt.Sprintf("parse manifest: %v", err))
		return
	}

	entryByKey := make(map[string]*batchPutManifestEntry, len(manifest.Entries))
	for i := range manifest.Entries {
		entryByKey[manifest.Entries[i].Key] = &manifest.Entries[i]
	}

	audit := auditMapFromContext(r, 0)

	var results []batchPutResult
	var nStored int
	for {
		hdr, err = tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			writeS3Error(w, 400, "InvalidRequest", fmt.Sprintf("read tar entry: %v", err))
			return
		}

		key := strings.TrimPrefix(hdr.Name, "data/")
		entry, ok := entryByKey[key]
		if !ok {
			results = append(results, batchPutResult{Key: key, Status: "error", Message: "not in manifest"})
			continue
		}

		data, err := io.ReadAll(tr)
		if err != nil {
			results = append(results, batchPutResult{Key: key, Status: "error", Message: err.Error()})
			continue
		}

		if err := storage.Put(key, data, entry.Metadata, audit); err != nil {
			results = append(results, batchPutResult{Key: key, Status: "error", Message: err.Error()})
			continue
		}
		results = append(results, batchPutResult{Key: key, Status: "stored"})
		nStored++
	}

	log.Printf("batch put: %d/%d stored", nStored, len(manifest.Entries))

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	json.NewEncoder(w).Encode(batchPutResponse{Results: results})
}
