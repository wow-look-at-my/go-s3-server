package main

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const maxMaxKeys = 10000

type ListBucketResult struct {
	XMLName               xml.Name   `xml:"ListBucketResult"`
	Xmlns                 string     `xml:"xmlns,attr"`
	Name                  string     `xml:"Name"`
	Prefix                string     `xml:"Prefix"`
	MaxKeys               int        `xml:"MaxKeys"`
	IsTruncated           bool       `xml:"IsTruncated"`
	Contents              []S3Object `xml:"Contents"`
	NextContinuationToken string     `xml:"NextContinuationToken,omitempty"`
}

type S3Object struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	Size         int64  `xml:"Size"`
}

type S3Error struct {
	XMLName xml.Name `xml:"Error"`
	Code    string   `xml:"Code"`
	Message string   `xml:"Message"`
}

func writeS3Error(w http.ResponseWriter, httpStatus int, code, message string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(httpStatus)
	xml.NewEncoder(w).Encode(S3Error{Code: code, Message: message})
}

func handleListObjectsV2(w http.ResponseWriter, r *http.Request, storage *Storage, bucket string) {
	prefix := r.URL.Query().Get("prefix")
	maxKeys := 1000
	if mk := r.URL.Query().Get("max-keys"); mk != "" {
		if v, err := strconv.Atoi(mk); err == nil && v > 0 {
			maxKeys = v
		}
	}
	if maxKeys > maxMaxKeys {
		maxKeys = maxMaxKeys
	}
	continuationToken := r.URL.Query().Get("continuation-token")

	result, err := storage.List(prefix, maxKeys, continuationToken)
	if err != nil {
		writeS3Error(w, 500, "InternalError", err.Error())
		return
	}

	xmlResult := ListBucketResult{
		Xmlns:       "http://s3.amazonaws.com/doc/2006-03-01/",
		Name:        bucket,
		Prefix:      prefix,
		MaxKeys:     maxKeys,
		IsTruncated: result.IsTruncated,
	}
	if result.NextContinuationToken != "" {
		xmlResult.NextContinuationToken = result.NextContinuationToken
	}
	for _, obj := range result.Objects {
		xmlResult.Contents = append(xmlResult.Contents, S3Object{
			Key:          obj.Key,
			LastModified: obj.LastModified.UTC().Format(time.RFC3339Nano),
			Size:         obj.Size,
		})
	}

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(200)
	w.Write([]byte(xml.Header))
	xml.NewEncoder(w).Encode(xmlResult)
}

func handleGetObject(w http.ResponseWriter, r *http.Request, storage *Storage, key string) {
	data, meta, err := storage.Get(key)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			writeS3Error(w, 404, "NoSuchKey", fmt.Sprintf("The specified key does not exist: %s", key))
			return
		}
		writeS3Error(w, 500, "InternalError", err.Error())
		return
	}

	for k, v := range meta.Metadata {
		// Capitalize first letter of metadata key
		name := k
		if len(name) > 0 {
			name = strings.ToUpper(name[:1]) + name[1:]
		}
		w.Header().Set("X-Amz-Meta-"+name, v)
	}
	w.Header().Set("Last-Modified", meta.ModTime.UTC().Format(http.TimeFormat))
	w.Header().Set("Content-Length", strconv.FormatInt(meta.Size, 10))
	w.WriteHeader(200)
	w.Write(data)
}

func handlePutObject(w http.ResponseWriter, r *http.Request, storage *Storage, key string) {
	data, err := io.ReadAll(r.Body)
	if err != nil {
		writeS3Error(w, 500, "InternalError", "failed to read body")
		return
	}

	meta := make(map[string]string)
	for k, vals := range r.Header {
		lk := strings.ToLower(k)
		if strings.HasPrefix(lk, "x-amz-meta-") {
			metaKey := strings.TrimPrefix(lk, "x-amz-meta-")
			meta[metaKey] = vals[0]
		}
	}

	audit := auditMapFromContext(r, int64(len(data)))

	if err := storage.Put(key, data, meta, audit); err != nil {
		if errors.Is(err, ErrWriteOnceConflict) || errors.Is(err, ErrWriteOnceDuplicate) {
			writeS3Error(w, 409, "ConflictException", err.Error())
			return
		}
		writeS3Error(w, 500, "InternalError", err.Error())
		return
	}

	w.WriteHeader(200)
}

// auditMapFromContext converts per-request audit info into a flat map that
// storage.Put can persist as extended attributes on the uploaded object.
// These fields answer: who uploaded this, when, from where, and with what
// client — the data needed to investigate a suspected compromise.
func auditMapFromContext(r *http.Request, size int64) map[string]string {
	a := auditFromContext(r.Context())
	if a == nil {
		return nil
	}
	m := map[string]string{
		"uploader":       a.Username,
		"uploaded_at":    a.Timestamp.UTC().Format(time.RFC3339Nano),
		"client_ip":      a.ClientIP,
		"user_agent":     a.UserAgent,
		"content_length": strconv.FormatInt(size, 10),
	}
	return m
}
