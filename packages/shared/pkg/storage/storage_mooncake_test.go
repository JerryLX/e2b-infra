package storage

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeMooncakeClient struct {
	mu      sync.Mutex
	objects map[string][]byte
}

func newFakeMooncakeClient() *fakeMooncakeClient {
	return &fakeMooncakeClient{objects: make(map[string][]byte)}
}

func (f *fakeMooncakeClient) Put(_ context.Context, key string, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	copied := make([]byte, len(data))
	copy(copied, data)
	f.objects[key] = copied

	return nil
}

func (f *fakeMooncakeClient) PutReader(_ context.Context, key string, data io.Reader, _ int64) error {
	b, err := io.ReadAll(data)
	if err != nil {
		return err
	}

	return f.Put(context.Background(), key, b)
}

func (f *fakeMooncakeClient) Get(_ context.Context, key string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	data, ok := f.objects[key]
	if !ok {
		return nil, ErrObjectNotExist
	}

	copied := make([]byte, len(data))
	copy(copied, data)

	return copied, nil
}

func (f *fakeMooncakeClient) ReadInto(ctx context.Context, key string, dst []byte) (int, error) {
	data, err := f.Get(ctx, key)
	if err != nil {
		return 0, err
	}

	n := copy(dst, data)
	if n < len(dst) {
		return n, io.EOF
	}

	return n, nil
}

func (f *fakeMooncakeClient) Exists(_ context.Context, key string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	_, ok := f.objects[key]

	return ok, nil
}

func (f *fakeMooncakeClient) Delete(_ context.Context, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	delete(f.objects, key)

	return nil
}

func (f *fakeMooncakeClient) DeletePrefix(_ context.Context, prefix string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	for key := range f.objects {
		if strings.HasPrefix(key, prefix) {
			delete(f.objects, key)
		}
	}

	return nil
}

func (f *fakeMooncakeClient) keys() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	keys := make([]string, 0, len(f.objects))
	for key := range f.objects {
		keys = append(keys, key)
	}

	return keys
}

func TestMooncakeBlobStoresWholeObject(t *testing.T) {
	t.Parallel()

	client := newFakeMooncakeClient()
	provider := newMooncakeStorage(client, "templates", MemoryChunkSize, 2)

	blob, err := provider.OpenBlob(t.Context(), "build/metadata.json", MetadataObjectType)
	require.NoError(t, err)

	require.NoError(t, blob.Put(t.Context(), []byte(`{"ok":true}`)))

	data, err := GetBlob(t.Context(), blob)
	require.NoError(t, err)
	assert.Equal(t, `{"ok":true}`, string(data))

	exists, err := blob.Exists(t.Context())
	require.NoError(t, err)
	assert.True(t, exists)

	assert.ElementsMatch(t, []string{"templates/build/metadata.json"}, client.keys())
}

func TestMooncakeSeekableStoresManifestAndBlocks(t *testing.T) {
	t.Parallel()

	client := newFakeMooncakeClient()
	blockSize := int64(4)
	provider := newMooncakeStorage(client, "templates", blockSize, 2)

	srcPath := filepath.Join(t.TempDir(), "rootfs.ext4")
	require.NoError(t, os.WriteFile(srcPath, []byte("abcdefghijkl"), 0o600))

	seekableObject, err := provider.OpenSeekable(t.Context(), "build/rootfs.ext4", RootFSObjectType)
	require.NoError(t, err)
	seekable := seekableObject.(*mooncakeSeekable)

	_, _, err = seekable.StoreFile(t.Context(), srcPath)
	require.NoError(t, err)

	assert.ElementsMatch(t, []string{
		"templates/build/rootfs.ext4.manifest",
		"templates/build/rootfs.ext4.blocks/000000000000",
		"templates/build/rootfs.ext4.blocks/000000000001",
		"templates/build/rootfs.ext4.blocks/000000000002",
	}, client.keys())

	size, err := seekable.Size(t.Context())
	require.NoError(t, err)
	assert.Equal(t, int64(12), size)

	buf := make([]byte, 7)
	n, err := seekable.ReadAt(t.Context(), buf, 3, nil)
	require.NoError(t, err)
	assert.Equal(t, 7, n)
	assert.Equal(t, "defghij", string(buf))
}

func TestMooncakeSeekableReadAtEOF(t *testing.T) {
	t.Parallel()

	client := newFakeMooncakeClient()
	provider := newMooncakeStorage(client, "templates", 4, 1)

	srcPath := filepath.Join(t.TempDir(), "memfile")
	require.NoError(t, os.WriteFile(srcPath, []byte("abcdef"), 0o600))

	seekableObject, err := provider.OpenSeekable(t.Context(), "build/memfile", MemfileObjectType)
	require.NoError(t, err)
	seekable := seekableObject.(*mooncakeSeekable)
	_, _, err = seekable.StoreFile(t.Context(), srcPath)
	require.NoError(t, err)

	buf := make([]byte, 4)
	n, err := seekable.ReadAt(t.Context(), buf, 4, nil)
	require.ErrorIs(t, err, io.EOF)
	assert.Equal(t, 2, n)
	assert.Equal(t, []byte{'e', 'f', 0, 0}, buf)
}

func TestMooncakeSeekableOpenRangeReader(t *testing.T) {
	t.Parallel()

	client := newFakeMooncakeClient()
	provider := newMooncakeStorage(client, "templates", 4, 1)

	srcPath := filepath.Join(t.TempDir(), "rootfs.ext4")
	require.NoError(t, os.WriteFile(srcPath, []byte("abcdefghijkl"), 0o600))

	seekableObject, err := provider.OpenSeekable(t.Context(), "build/rootfs.ext4", RootFSObjectType)
	require.NoError(t, err)
	seekable := seekableObject.(*mooncakeSeekable)
	_, _, err = seekable.StoreFile(t.Context(), srcPath)
	require.NoError(t, err)

	reader, err := seekable.OpenRangeReader(t.Context(), 2, 8, nil)
	require.NoError(t, err)
	defer reader.Close()

	var out bytes.Buffer
	_, err = io.Copy(&out, reader)
	require.NoError(t, err)
	assert.Equal(t, "cdefghij", out.String())
}

func TestMooncakeDeleteObjectsWithPrefix(t *testing.T) {
	t.Parallel()

	client := newFakeMooncakeClient()
	provider := newMooncakeStorage(client, "templates", 4, 1)

	blob, err := provider.OpenBlob(t.Context(), "build/metadata.json", MetadataObjectType)
	require.NoError(t, err)
	require.NoError(t, blob.Put(t.Context(), []byte("data")))

	other, err := provider.OpenBlob(t.Context(), "other/metadata.json", MetadataObjectType)
	require.NoError(t, err)
	require.NoError(t, other.Put(t.Context(), []byte("data")))

	build2, err := provider.OpenBlob(t.Context(), "build2/metadata.json", MetadataObjectType)
	require.NoError(t, err)
	require.NoError(t, build2.Put(t.Context(), []byte("data")))

	require.NoError(t, provider.DeleteObjectsWithPrefix(t.Context(), "build"))

	exists, err := blob.Exists(t.Context())
	require.NoError(t, err)
	assert.False(t, exists)

	exists, err = other.Exists(t.Context())
	require.NoError(t, err)
	assert.True(t, exists)

	exists, err = build2.Exists(t.Context())
	require.NoError(t, err)
	assert.True(t, exists)
}

func TestMooncakeMissingObject(t *testing.T) {
	t.Parallel()

	client := newFakeMooncakeClient()
	provider := newMooncakeStorage(client, "templates", 4, 1)

	blob, err := provider.OpenBlob(t.Context(), "missing", MetadataObjectType)
	require.NoError(t, err)

	_, err = GetBlob(t.Context(), blob)
	require.True(t, errors.Is(err, ErrObjectNotExist))
}

func TestGetStorageProviderMooncakeUsesNamespaceWithoutBucket(t *testing.T) {
	t.Setenv(storageProviderEnv, string(MooncakeProvider))
	t.Setenv(mooncakeEndpointEnv, "http://moon.example")
	t.Setenv(mooncakeNamespaceEnv, "templates")

	provider, err := GetStorageProvider(t.Context(), StorageConfig{
		GetBucketName: func() string {
			require.FailNow(t, "bucket name should not be read when MOONCAKE_NAMESPACE is set")
			return ""
		},
	})
	require.NoError(t, err)
	assert.Contains(t, provider.GetDetails(), "templates")
}
