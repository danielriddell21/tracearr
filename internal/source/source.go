// Package source converts vendor-specific webhook/script payloads into
// the vendor-agnostic correlate.Event type. One file per app; the package
// itself holds shared helpers.
package source

import "strconv"

func atoi(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}
