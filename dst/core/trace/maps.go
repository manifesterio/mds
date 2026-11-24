package trace

import "slices"

// StringMap returns a hash of m that does not depend on map order: the
// number of entries, then each key and its value in sorted key order. It is
// for StateHash methods of small maps.
func StringMap(m map[string]string) uint64 {
	keys := make([]string, 0, len(m))
	for k := range m { //determinismcheck:ok the keys are sorted before use
		keys = append(keys, k)
	}
	slices.Sort(keys)
	h := New()
	h.Uint64(uint64(len(keys)))
	for _, k := range keys {
		h.String(k)
		h.String(m[k])
	}
	return h.Sum()
}
