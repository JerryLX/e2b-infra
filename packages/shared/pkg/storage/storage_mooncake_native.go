//go:build mooncake_cgo

package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"unsafe"

	store "github.com/kvcache-ai/Mooncake/mooncake-store/go/mooncakestore"
)

type nativeMooncakeClient struct {
	store *store.Store
}

var _ mooncakeClient = (*nativeMooncakeClient)(nil)
var _ mooncakeRegisteringClient = (*nativeMooncakeClient)(nil)

func newNativeMooncakeClientFromEnv() (mooncakeClient, error) {
	metadataServer := envString(mooncakeMetadataServerEnv, "")
	if metadataServer == "" {
		return nil, fmt.Errorf("%s is required when %s=true", mooncakeMetadataServerEnv, mooncakeNativeEnabledEnv)
	}
	masterAddr := envString(mooncakeMasterAddrEnv, "")
	if masterAddr == "" {
		return nil, fmt.Errorf("%s is required when %s=true", mooncakeMasterAddrEnv, mooncakeNativeEnabledEnv)
	}

	globalSegmentSize, err := mooncakeEnvUint64(mooncakeGlobalSegmentSizeEnv, defaultMooncakeGlobalSegmentSize)
	if err != nil {
		return nil, err
	}
	localBufferSize, err := mooncakeEnvUint64(mooncakeLocalBufferSizeEnv, defaultMooncakeLocalBufferSize)
	if err != nil {
		return nil, err
	}
	memPoolSize, err := mooncakeEnvUint64(mooncakeMemPoolSizeEnv, 0)
	if err != nil {
		return nil, err
	}

	s, err := store.New()
	if err != nil {
		return nil, fmt.Errorf("failed to create mooncake store client: %w", err)
	}

	err = s.Setup(
		envString(mooncakeLocalHostnameEnv, "localhost"),
		metadataServer,
		globalSegmentSize,
		localBufferSize,
		envString(mooncakeProtocolEnv, "urma"),
		envString(mooncakeDeviceNameEnv, ""),
		masterAddr,
		memPoolSize,
		envString(mooncakeServerAddressEnv, ""),
		envString(mooncakeIPCSocketPathEnv, ""),
	)
	if err != nil {
		s.Close()

		return nil, fmt.Errorf("failed to setup mooncake store client: %w", err)
	}

	return &nativeMooncakeClient{store: s}, nil
}

func (c *nativeMooncakeClient) Put(_ context.Context, key string, data []byte) error {
	if err := c.store.Put(key, data, nil); err != nil {
		return fmt.Errorf("mooncake put %q: %w", key, err)
	}

	return nil
}

func (c *nativeMooncakeClient) PutReader(ctx context.Context, key string, data io.Reader, size int64) error {
	if size < 0 {
		return fmt.Errorf("size must be non-negative")
	}

	buf, err := io.ReadAll(data)
	if err != nil {
		return err
	}

	return c.Put(ctx, key, buf)
}

func (c *nativeMooncakeClient) Get(ctx context.Context, key string) ([]byte, error) {
	size, err := c.store.GetSize(key)
	if err != nil {
		exists, existsErr := c.Exists(ctx, key)
		if existsErr == nil && !exists {
			return nil, ErrObjectNotExist
		}

		return nil, fmt.Errorf("mooncake get size %q: %w", key, err)
	}

	buf := make([]byte, size)
	n, err := c.ReadInto(ctx, key, buf)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}

	return buf[:n], nil
}

func (c *nativeMooncakeClient) ReadInto(ctx context.Context, key string, dst []byte) (int, error) {
	if len(dst) == 0 {
		return 0, ErrBufferTooSmall
	}

	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}

	n, err := c.store.Get(key, dst)
	if err != nil {
		exists, existsErr := c.Exists(ctx, key)
		if existsErr == nil && !exists {
			return 0, ErrObjectNotExist
		}

		return 0, fmt.Errorf("mooncake get %q: %w", key, err)
	}
	if n < int64(len(dst)) {
		return int(n), io.EOF
	}

	return int(n), nil
}

func (c *nativeMooncakeClient) ReadIntoRegistered(ctx context.Context, key string, dst []byte) (int, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}

	n, err := c.store.GetInto(key, uintptrPointer(dst), uint64(len(dst)))
	if err != nil {
		exists, existsErr := c.Exists(ctx, key)
		if existsErr == nil && !exists {
			return 0, ErrObjectNotExist
		}

		return 0, fmt.Errorf("mooncake get %q: %w", key, err)
	}
	if n < int64(len(dst)) {
		return int(n), io.EOF
	}

	return int(n), nil
}

func (c *nativeMooncakeClient) Exists(_ context.Context, key string) (bool, error) {
	exists, err := c.store.Exists(key)
	if err != nil {
		return false, fmt.Errorf("mooncake exists %q: %w", key, err)
	}

	return exists, nil
}

func (c *nativeMooncakeClient) Delete(_ context.Context, key string) error {
	if err := c.store.Remove(key, false); err != nil {
		return fmt.Errorf("mooncake delete %q: %w", key, err)
	}

	return nil
}

func (c *nativeMooncakeClient) DeletePrefix(_ context.Context, prefix string) error {
	pattern := "^" + regexp.QuoteMeta(prefix) + ".*"
	if _, err := c.store.RemoveByRegex(pattern, false); err != nil {
		return fmt.Errorf("mooncake delete prefix %q: %w", prefix, err)
	}

	return nil
}

func (c *nativeMooncakeClient) RegisterBuffer(ptr uintptr, size uint64) error {
	if err := c.store.RegisterBuffer(ptr, size); err != nil {
		return fmt.Errorf("mooncake register buffer %#x/%d: %w", ptr, size, err)
	}

	return nil
}

func (c *nativeMooncakeClient) UnregisterBuffer(ptr uintptr) error {
	if err := c.store.UnregisterBuffer(ptr); err != nil {
		return fmt.Errorf("mooncake unregister buffer %#x: %w", ptr, err)
	}

	return nil
}

func envString(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}

	return fallback
}

func uintptrPointer(b []byte) uintptr {
	return uintptr(unsafe.Pointer(&b[0]))
}
