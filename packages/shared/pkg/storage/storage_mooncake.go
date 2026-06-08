package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"github.com/e2b-dev/infra/packages/shared/pkg/env"
	"github.com/e2b-dev/infra/packages/shared/pkg/utils"
)

const (
	mooncakeEndpointEnv          = "MOONCAKE_HTTP_ENDPOINT"
	mooncakeNamespaceEnv         = "MOONCAKE_NAMESPACE"
	mooncakeWriteTimeoutEnv      = "MOONCAKE_WRITE_TIMEOUT_SECONDS"
	mooncakeReadTimeoutEnv       = "MOONCAKE_READ_TIMEOUT_SECONDS"
	mooncakeOperationTimeoutEnv  = "MOONCAKE_OPERATION_TIMEOUT_SECONDS"
	mooncakeUploadConcurrencyEnv = "MOONCAKE_UPLOAD_CONCURRENCY"

	defaultMooncakeWriteTimeoutSeconds     = 30
	defaultMooncakeReadTimeoutSeconds      = 15
	defaultMooncakeOperationTimeoutSeconds = 5
	defaultMooncakeUploadConcurrency       = 16

	mooncakeManifestSuffix = ".manifest"
	mooncakeBlocksDir      = ".blocks"
)

type mooncakeClient interface {
	Put(ctx context.Context, key string, data []byte) error
	PutReader(ctx context.Context, key string, data io.Reader, size int64) error
	Get(ctx context.Context, key string) ([]byte, error)
	ReadInto(ctx context.Context, key string, dst []byte) (int, error)
	Exists(ctx context.Context, key string) (bool, error)
	Delete(ctx context.Context, key string) error
	DeletePrefix(ctx context.Context, prefix string) error
}

type mooncakeStorage struct {
	client            mooncakeClient
	namespace         string
	blockSize         int64
	uploadConcurrency int
}

var _ StorageProvider = (*mooncakeStorage)(nil)

type mooncakeBlob struct {
	client mooncakeClient
	key    string
}

var _ Blob = (*mooncakeBlob)(nil)

type mooncakeSeekable struct {
	client            mooncakeClient
	key               string
	blockSize         int64
	uploadConcurrency int
}

var (
	_ Seekable        = (*mooncakeSeekable)(nil)
	_ StreamingReader = (*mooncakeSeekable)(nil)
)

type mooncakeManifest struct {
	Version    int    `json:"version"`
	Path       string `json:"path"`
	Size       int64  `json:"size"`
	BlockSize  int64  `json:"block_size"`
	BlockCount int64  `json:"block_count"`
}

func newMooncakeStorageFromEnv(cfg StorageConfig) (*mooncakeStorage, error) {
	endpoint := os.Getenv(mooncakeEndpointEnv)
	if endpoint == "" {
		return nil, fmt.Errorf("%s is required when STORAGE_PROVIDER=%s", mooncakeEndpointEnv, MooncakeProvider)
	}

	namespace := os.Getenv(mooncakeNamespaceEnv)
	if namespace == "" {
		namespace = cfg.GetBucketName()
	}

	writeTimeoutSeconds, err := env.GetEnvAsInt(mooncakeWriteTimeoutEnv, defaultMooncakeWriteTimeoutSeconds)
	if err != nil {
		return nil, fmt.Errorf("failed to parse %s: %w", mooncakeWriteTimeoutEnv, err)
	}

	readTimeoutSeconds, err := env.GetEnvAsInt(mooncakeReadTimeoutEnv, defaultMooncakeReadTimeoutSeconds)
	if err != nil {
		return nil, fmt.Errorf("failed to parse %s: %w", mooncakeReadTimeoutEnv, err)
	}

	operationTimeoutSeconds, err := env.GetEnvAsInt(mooncakeOperationTimeoutEnv, defaultMooncakeOperationTimeoutSeconds)
	if err != nil {
		return nil, fmt.Errorf("failed to parse %s: %w", mooncakeOperationTimeoutEnv, err)
	}

	uploadConcurrency, err := env.GetEnvAsInt(mooncakeUploadConcurrencyEnv, defaultMooncakeUploadConcurrency)
	if err != nil {
		return nil, fmt.Errorf("failed to parse %s: %w", mooncakeUploadConcurrencyEnv, err)
	}
	if uploadConcurrency <= 0 {
		uploadConcurrency = defaultMooncakeUploadConcurrency
	}

	return newMooncakeStorage(
		newHTTPMooncakeClient(
			strings.TrimRight(endpoint, "/"),
			time.Duration(writeTimeoutSeconds)*time.Second,
			time.Duration(readTimeoutSeconds)*time.Second,
			time.Duration(operationTimeoutSeconds)*time.Second,
		),
		namespace,
		MemoryChunkSize,
		uploadConcurrency,
	), nil
}

func newMooncakeStorage(client mooncakeClient, namespace string, blockSize int64, uploadConcurrency int) *mooncakeStorage {
	if uploadConcurrency <= 0 {
		uploadConcurrency = defaultMooncakeUploadConcurrency
	}

	return &mooncakeStorage{
		client:            client,
		namespace:         strings.Trim(namespace, "/"),
		blockSize:         blockSize,
		uploadConcurrency: uploadConcurrency,
	}
}

func (s *mooncakeStorage) DeleteObjectsWithPrefix(ctx context.Context, prefix string) error {
	key := s.makeKey(prefix)
	if err := s.client.Delete(ctx, key); ignoreNotExists(err) != nil {
		return err
	}

	return s.client.DeletePrefix(ctx, key+"/")
}

func (s *mooncakeStorage) UploadSignedURL(context.Context, string, time.Duration) (string, error) {
	return "", errors.New("mooncake storage does not support signed upload URLs")
}

func (s *mooncakeStorage) OpenBlob(_ context.Context, objectPath string, _ ObjectType) (Blob, error) {
	return &mooncakeBlob{
		client: s.client,
		key:    s.makeKey(objectPath),
	}, nil
}

func (s *mooncakeStorage) OpenSeekable(_ context.Context, objectPath string, _ SeekableObjectType) (Seekable, error) {
	return &mooncakeSeekable{
		client:            s.client,
		key:               s.makeKey(objectPath),
		blockSize:         s.blockSize,
		uploadConcurrency: s.uploadConcurrency,
	}, nil
}

func (s *mooncakeStorage) GetDetails() string {
	return fmt.Sprintf("[Mooncake Storage, namespace set to %s]", s.namespace)
}

func (s *mooncakeStorage) makeKey(objectPath string) string {
	cleanPath := strings.Trim(objectPath, "/")
	if s.namespace == "" {
		return cleanPath
	}

	return path.Join(s.namespace, cleanPath)
}

func (b *mooncakeBlob) WriteTo(ctx context.Context, dst io.Writer) (int64, error) {
	data, err := b.client.Get(ctx, b.key)
	if err != nil {
		return 0, err
	}

	return io.Copy(dst, bytes.NewReader(data))
}

func (b *mooncakeBlob) Put(ctx context.Context, data []byte, _ ...PutOption) error {
	return b.client.Put(ctx, b.key, data)
}

func (b *mooncakeBlob) Exists(ctx context.Context) (bool, error) {
	return b.client.Exists(ctx, b.key)
}

func (s *mooncakeSeekable) ReadAt(ctx context.Context, buffer []byte, off int64, _ *FrameTable) (int, error) {
	if len(buffer) == 0 {
		return 0, ErrBufferTooSmall
	}
	if off < 0 {
		return 0, fmt.Errorf("offset must be non-negative")
	}

	manifest, err := s.loadManifest(ctx)
	if err != nil {
		return 0, err
	}
	if off >= manifest.Size {
		return 0, io.EOF
	}

	remaining := int64(len(buffer))
	if off+remaining > manifest.Size {
		remaining = manifest.Size - off
	}

	total := 0
	for remaining > 0 {
		blockIdx := off / manifest.BlockSize
		blockOffset := off % manifest.BlockSize
		readLen := min(remaining, manifest.BlockSize-blockOffset)

		n, err := s.readBlockInto(ctx, blockIdx, blockOffset, buffer[total:total+int(readLen)])
		if err != nil {
			return total, err
		}
		total += n
		off += int64(n)
		remaining -= int64(n)

		if int64(n) < readLen {
			return total, io.EOF
		}
	}

	if total < len(buffer) {
		return total, io.EOF
	}

	return total, nil
}

func (s *mooncakeSeekable) readBlockInto(ctx context.Context, blockIdx, blockOffset int64, dst []byte) (int, error) {
	if blockOffset == 0 {
		return s.client.ReadInto(ctx, s.blockKey(blockIdx), dst)
	}

	block, err := s.client.Get(ctx, s.blockKey(blockIdx))
	if err != nil {
		return 0, err
	}
	if blockOffset >= int64(len(block)) {
		return 0, io.EOF
	}

	available := int64(len(block)) - blockOffset
	readLen := min(int64(len(dst)), available)
	n := copy(dst[:readLen], block[blockOffset:blockOffset+readLen])
	if n < len(dst) {
		return n, io.EOF
	}

	return n, nil
}

func (s *mooncakeSeekable) Size(ctx context.Context) (int64, error) {
	manifest, err := s.loadManifest(ctx)
	if err != nil {
		return 0, err
	}

	return manifest.Size, nil
}

func (s *mooncakeSeekable) OpenRangeReader(ctx context.Context, off, length int64, frameTable *FrameTable) (io.ReadCloser, error) {
	if frameTable.IsCompressed() {
		return nil, errors.New("mooncake storage does not support compressed seekable objects yet")
	}
	if length < 0 {
		return nil, fmt.Errorf("length must be non-negative")
	}

	data := make([]byte, length)
	n, err := s.ReadAt(ctx, data, off, nil)
	if ignoreEOF(err) != nil {
		return nil, err
	}
	if errors.Is(err, io.EOF) {
		err = nil
	}

	return io.NopCloser(bytes.NewReader(data[:n])), err
}

func (s *mooncakeSeekable) StoreFile(ctx context.Context, filePath string, opts ...PutOption) (*FrameTable, [32]byte, error) {
	putOpts := ApplyPutOptions(opts)
	if CompressConfigFromOpts(putOpts).IsCompressionEnabled() {
		return nil, [32]byte{}, errors.New("mooncake storage does not support compressed seekable objects yet")
	}

	input, err := os.Open(filePath)
	if err != nil {
		return nil, [32]byte{}, fmt.Errorf("failed to open file %s: %w", filePath, err)
	}
	defer input.Close()

	stat, err := input.Stat()
	if err != nil {
		return nil, [32]byte{}, fmt.Errorf("failed to stat file %s: %w", filePath, err)
	}

	size := stat.Size()
	blockCount := (size + s.blockSize - 1) / s.blockSize

	var checksum [32]byte
	if putOpts.Checksum {
		hasher := sha256.New()
		if _, err := input.Seek(0, io.SeekStart); err != nil {
			return nil, [32]byte{}, fmt.Errorf("failed to seek file %s: %w", filePath, err)
		}
		if _, err := io.Copy(hasher, input); err != nil {
			return nil, [32]byte{}, fmt.Errorf("failed to checksum file %s: %w", filePath, err)
		}
		copy(checksum[:], hasher.Sum(nil))
	}

	ec := utils.NewErrorCollector(s.uploadConcurrency)
	for blockIdx := int64(0); blockIdx < blockCount; blockIdx++ {
		blockIdx := blockIdx
		ec.Go(ctx, func() error {
			offset := blockIdx * s.blockSize
			readLen := min(s.blockSize, size-offset)

			if err := s.client.PutReader(ctx, s.blockKey(blockIdx), io.NewSectionReader(input, offset, readLen), readLen); err != nil {
				return fmt.Errorf("failed to write mooncake block %d: %w", blockIdx, err)
			}

			return nil
		})
	}

	if err := ec.Wait(); err != nil {
		return nil, [32]byte{}, err
	}

	manifest := mooncakeManifest{
		Version:    1,
		Path:       s.key,
		Size:       size,
		BlockSize:  s.blockSize,
		BlockCount: blockCount,
	}
	manifestData, err := json.Marshal(manifest)
	if err != nil {
		return nil, [32]byte{}, fmt.Errorf("failed to marshal mooncake manifest: %w", err)
	}

	if err := s.client.Put(ctx, s.manifestKey(), manifestData); err != nil {
		return nil, [32]byte{}, fmt.Errorf("failed to write mooncake manifest: %w", err)
	}

	return nil, checksum, nil
}

func (s *mooncakeSeekable) readBlock(input *os.File, blockIdx int64, fileSize int64) ([]byte, error) {
	offset := blockIdx * s.blockSize
	readLen := min(s.blockSize, fileSize-offset)
	if readLen < 0 {
		return nil, fmt.Errorf("block %d starts beyond file size %d", blockIdx, fileSize)
	}

	data := make([]byte, readLen)
	n, err := input.ReadAt(data, offset)
	if errors.Is(err, io.EOF) && int64(n) == readLen {
		err = nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to read block %d: %w", blockIdx, err)
	}

	return data[:n], nil
}

func (s *mooncakeSeekable) loadManifest(ctx context.Context) (*mooncakeManifest, error) {
	data, err := s.client.Get(ctx, s.manifestKey())
	if err != nil {
		return nil, err
	}

	var manifest mooncakeManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, fmt.Errorf("failed to unmarshal mooncake manifest: %w", err)
	}
	if manifest.BlockSize <= 0 {
		return nil, fmt.Errorf("invalid mooncake manifest block size: %d", manifest.BlockSize)
	}

	return &manifest, nil
}

func (s *mooncakeSeekable) manifestKey() string {
	return s.key + mooncakeManifestSuffix
}

func (s *mooncakeSeekable) blockKey(idx int64) string {
	return fmt.Sprintf("%s%s/%012d", s.key, mooncakeBlocksDir, idx)
}

type httpMooncakeClient struct {
	endpoint         string
	client           *http.Client
	writeTimeout     time.Duration
	readTimeout      time.Duration
	operationTimeout time.Duration
}

func newHTTPMooncakeClient(endpoint string, writeTimeout, readTimeout, operationTimeout time.Duration) *httpMooncakeClient {
	return &httpMooncakeClient{
		endpoint:         endpoint,
		client:           &http.Client{},
		writeTimeout:     writeTimeout,
		readTimeout:      readTimeout,
		operationTimeout: operationTimeout,
	}
}

func (c *httpMooncakeClient) Put(ctx context.Context, key string, data []byte) error {
	return c.PutReader(ctx, key, bytes.NewReader(data), int64(len(data)))
}

func (c *httpMooncakeClient) PutReader(ctx context.Context, key string, data io.Reader, size int64) error {
	ctx, cancel := context.WithTimeout(ctx, c.writeTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.objectURL(key), data)
	if err != nil {
		return err
	}
	req.ContentLength = size

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return mooncakeHTTPStatusError(resp, http.StatusOK, http.StatusCreated, http.StatusNoContent)
}

func (c *httpMooncakeClient) Get(ctx context.Context, key string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, c.readTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.objectURL(key), nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrObjectNotExist
	}
	if err := mooncakeHTTPStatusError(resp, http.StatusOK); err != nil {
		return nil, err
	}

	return io.ReadAll(resp.Body)
}

func (c *httpMooncakeClient) ReadInto(ctx context.Context, key string, dst []byte) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, c.readTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.objectURL(key), nil)
	if err != nil {
		return 0, err
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return 0, ErrObjectNotExist
	}
	if err := mooncakeHTTPStatusError(resp, http.StatusOK); err != nil {
		return 0, err
	}

	n, err := io.ReadFull(resp.Body, dst)
	if errors.Is(err, io.ErrUnexpectedEOF) {
		err = io.EOF
	}

	return n, err
}

func (c *httpMooncakeClient) Exists(ctx context.Context, key string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, c.operationTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodHead, c.objectURL(key), nil)
	if err != nil {
		return false, err
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return false, nil
	}
	if err := mooncakeHTTPStatusError(resp, http.StatusOK, http.StatusNoContent); err != nil {
		return false, err
	}

	return true, nil
}

func (c *httpMooncakeClient) Delete(ctx context.Context, key string) error {
	ctx, cancel := context.WithTimeout(ctx, c.operationTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.objectURL(key), nil)
	if err != nil {
		return err
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return mooncakeHTTPStatusError(resp, http.StatusOK, http.StatusNoContent, http.StatusNotFound)
}

func (c *httpMooncakeClient) DeletePrefix(ctx context.Context, prefix string) error {
	ctx, cancel := context.WithTimeout(ctx, c.operationTimeout)
	defer cancel()

	u := c.endpoint + "/objects?prefix=" + url.QueryEscape(prefix)
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, u, nil)
	if err != nil {
		return err
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return mooncakeHTTPStatusError(resp, http.StatusOK, http.StatusNoContent, http.StatusNotFound)
}

func (c *httpMooncakeClient) objectURL(key string) string {
	return c.endpoint + "/objects/" + url.PathEscape(key)
}

func mooncakeHTTPStatusError(resp *http.Response, okStatuses ...int) error {
	for _, status := range okStatuses {
		if resp.StatusCode == status {
			return nil
		}
	}

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if len(body) > 0 {
		return fmt.Errorf("mooncake http request failed: status %d: %s", resp.StatusCode, string(body))
	}

	return fmt.Errorf("mooncake http request failed: status %d", resp.StatusCode)
}
