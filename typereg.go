package dna

import (
	"reflect"
	"sync"
)

var typeRegistryMu sync.RWMutex

var typeRegistry = map[string]reflect.Type{
	"bool":    reflect.TypeOf(false),
	"int":     reflect.TypeOf(int(0)),
	"int8":    reflect.TypeOf(int8(0)),
	"int16":   reflect.TypeOf(int16(0)),
	"int32":   reflect.TypeOf(int32(0)),
	"int64":   reflect.TypeOf(int64(0)),
	"uint":    reflect.TypeOf(uint(0)),
	"uint8":   reflect.TypeOf(uint8(0)),
	"uint16":  reflect.TypeOf(uint16(0)),
	"uint32":  reflect.TypeOf(uint32(0)),
	"uint64":  reflect.TypeOf(uint64(0)),
	"float32": reflect.TypeOf(float32(0)),
	"float64": reflect.TypeOf(float64(0)),
	"string":  reflect.TypeOf(""),
	"[]uint8": reflect.TypeOf([]byte{}),
	"github.com/luisfurquim/dna.PK": reflect.TypeOf(PK(0)),
}

// typeString converts a reflect.Type to a stable, deterministic string representation.
// For primitive types it returns the Kind name.
// For named types from external packages it returns "pkgpath.Name".
// For slices it returns "[]" + element type string.
// For pointers it returns "*" + element type string.
func typeString(t reflect.Type) string {
	if t == nil {
		return ""
	}

	switch t.Kind() {
	case reflect.Slice:
		return "[]" + typeString(t.Elem())
	case reflect.Pointer:
		return "*" + typeString(t.Elem())
	}

	// For named types with a package path, use "pkgpath.Name" for stability
	if t.PkgPath() != "" {
		return t.PkgPath() + "." + t.Name()
	}

	// Builtin types: use Kind string
	return t.Kind().String()
}

// RegisterType registers a custom type in the type registry so it can be
// reconstructed during deserialization of canonical JSON.
// Drivers should call this in their init() for types like time.Time, big.Int, etc.
func RegisterType(t reflect.Type) {
	typeRegistryMu.Lock()
	defer typeRegistryMu.Unlock()
	typeRegistry[typeString(t)] = t
}

// lookupType retrieves a reflect.Type from the registry by its string representation.
func lookupType(s string) (reflect.Type, bool) {
	typeRegistryMu.RLock()
	defer typeRegistryMu.RUnlock()
	t, ok := typeRegistry[s]
	return t, ok
}
