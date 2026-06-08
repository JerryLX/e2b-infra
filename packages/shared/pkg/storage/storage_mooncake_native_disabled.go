//go:build !mooncake_cgo

package storage

import "fmt"

func newNativeMooncakeClientFromEnv() (mooncakeClient, error) {
	return nil, fmt.Errorf("%s=true requires building with -tags mooncake_cgo", mooncakeNativeEnabledEnv)
}
