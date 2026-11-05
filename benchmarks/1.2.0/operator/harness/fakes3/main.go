package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
)

// dechunk decodes an aws-chunked body (minio-go's streaming signature):
// "<hex>;chunk-signature=...\r\n<data>\r\n" ... "0;...\r\n[trailers]\r\n".
func dechunk(r io.Reader) ([]byte, error) {
	br := bufio.NewReader(r)
	var out bytes.Buffer
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			return nil, fmt.Errorf("chunk header: %w", err)
		}
		line = strings.TrimRight(line, "\r\n")
		if i := strings.IndexByte(line, ';'); i >= 0 {
			line = line[:i]
		}
		n, err := strconv.ParseInt(strings.TrimSpace(line), 16, 64)
		if err != nil {
			return nil, fmt.Errorf("chunk size %q: %w", line, err)
		}
		if n == 0 {
			return out.Bytes(), nil
		}
		if _, err := io.CopyN(&out, br, n); err != nil {
			return nil, err
		}
		if _, err := br.ReadString('\n'); err != nil {
			return nil, err
		}
	}
}

func main() {
	backend := s3mem.New()
	if err := backend.CreateBucket("backups"); err != nil {
		log.Fatal(err)
	}
	faker := gofakes3.New(backend)
	inner := faker.Server()
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sha := r.Header.Get("X-Amz-Content-Sha256")
		if (r.Method == http.MethodPut || r.Method == http.MethodPost) && (strings.HasPrefix(sha, "STREAMING-") || strings.Contains(r.Header.Get("Content-Encoding"), "aws-chunked")) {
			body, err := dechunk(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(body))
			r.ContentLength = int64(len(body))
			r.Header.Set("Content-Length", strconv.Itoa(len(body)))
			r.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
			r.Header.Del("X-Amz-Decoded-Content-Length")
			ce := strings.ReplaceAll(r.Header.Get("Content-Encoding"), "aws-chunked", "")
			ce = strings.Trim(ce, ", ")
			if ce == "" {
				r.Header.Del("Content-Encoding")
			} else {
				r.Header.Set("Content-Encoding", ce)
			}
			log.Printf("dechunked %s %s -> %d bytes", r.Method, r.URL.Path, len(body))
		}
		inner.ServeHTTP(w, r)
	})
	log.Fatal(http.ListenAndServe(":9000", h))
}
