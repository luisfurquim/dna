package dna

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
)

// canonicalFieldSpec serializes a single FieldSpec to deterministic JSON.
// Keys are in strict alphanumeric order: auto, fk, index, joinList, name, pk, prec, type.
// This is a manual serializer — never uses json.Marshal for the top-level structure.
func canonicalFieldSpec(fs FieldSpec) string {
	var sb strings.Builder
	sb.WriteByte('{')

	// auto
	fmt.Fprintf(&sb, `"auto":%s`, boolStr(fs.Auto))

	// fk
	fmt.Fprintf(&sb, `,"fk":%s`, quoteStr(fs.Fk))

	// index
	fmt.Fprintf(&sb, `,"index":%d`, fs.Index)

	// joinList
	fmt.Fprintf(&sb, `,"joinList":%s`, boolStr(fs.JoinList))

	// name
	fmt.Fprintf(&sb, `,"name":%s`, quoteStr(fs.Name))

	// pk
	fmt.Fprintf(&sb, `,"pk":%s`, boolStr(fs.PK))

	// prec — always as a JSON array of strings, empty array if nil
	sb.WriteString(`,"prec":`)
	if len(fs.Prec) == 0 {
		sb.WriteString("[]")
	} else {
		sb.WriteByte('[')
		for i, p := range fs.Prec {
			if i > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString(quoteStr(p))
		}
		sb.WriteByte(']')
	}

	// type
	fmt.Fprintf(&sb, `,"type":%s`, quoteStr(typeString(fs.Type)))

	sb.WriteByte('}')
	return sb.String()
}

// CanonicalJSON serializes a []FieldSpec to deterministic JSON.
// The output is an array of objects with keys in strict alphanumeric order.
// Same input always produces the same output.
func CanonicalJSON(fields []FieldSpec) string {
	var sb strings.Builder
	sb.WriteByte('[')
	for i, fs := range fields {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(canonicalFieldSpec(fs))
	}
	sb.WriteByte(']')
	return sb.String()
}

// VersionHash computes the SHA-256 hex digest of the canonical JSON of a field list.
func VersionHash(fields []FieldSpec) string {
	data := CanonicalJSON(fields)
	h := sha256.Sum256([]byte(data))
	return fmt.Sprintf("%x", h)
}

// deserializeFieldSpecs parses canonical JSON back into []FieldSpec.
// Uses the type registry to reconstruct reflect.Type from stored type strings.
func deserializeFieldSpecs(jsonStr string) ([]FieldSpec, error) {
	var rawList []map[string]json.RawMessage
	if err := json.Unmarshal([]byte(jsonStr), &rawList); err != nil {
		return nil, fmt.Errorf("deserializeFieldSpecs: %w", err)
	}

	result := make([]FieldSpec, 0, len(rawList))

	for _, raw := range rawList {
		var fs FieldSpec

		if v, ok := raw["auto"]; ok {
			var b bool
			if err := json.Unmarshal(v, &b); err != nil {
				return nil, fmt.Errorf("deserializeFieldSpecs: field 'auto': %w", err)
			}
			fs.Auto = b
		}

		if v, ok := raw["fk"]; ok {
			var s string
			if err := json.Unmarshal(v, &s); err != nil {
				return nil, fmt.Errorf("deserializeFieldSpecs: field 'fk': %w", err)
			}
			fs.Fk = s
		}

		if v, ok := raw["index"]; ok {
			var n int
			if err := json.Unmarshal(v, &n); err != nil {
				return nil, fmt.Errorf("deserializeFieldSpecs: field 'index': %w", err)
			}
			fs.Index = n
		}

		if v, ok := raw["joinList"]; ok {
			var b bool
			if err := json.Unmarshal(v, &b); err != nil {
				return nil, fmt.Errorf("deserializeFieldSpecs: field 'joinList': %w", err)
			}
			fs.JoinList = b
		}

		if v, ok := raw["name"]; ok {
			var s string
			if err := json.Unmarshal(v, &s); err != nil {
				return nil, fmt.Errorf("deserializeFieldSpecs: field 'name': %w", err)
			}
			fs.Name = s
		}

		if v, ok := raw["pk"]; ok {
			var b bool
			if err := json.Unmarshal(v, &b); err != nil {
				return nil, fmt.Errorf("deserializeFieldSpecs: field 'pk': %w", err)
			}
			fs.PK = b
		}

		if v, ok := raw["prec"]; ok {
			var arr []string
			if err := json.Unmarshal(v, &arr); err != nil {
				return nil, fmt.Errorf("deserializeFieldSpecs: field 'prec': %w", err)
			}
			fs.Prec = arr
		}

		if v, ok := raw["type"]; ok {
			var s string
			if err := json.Unmarshal(v, &s); err != nil {
				return nil, fmt.Errorf("deserializeFieldSpecs: field 'type': %w", err)
			}
			if t, found := lookupType(s); found {
				fs.Type = t
			}
			// If type not found in registry, fs.Type remains nil.
			// This is acceptable for diff comparison — the type string
			// will still differ and trigger a migration.
		}

		result = append(result, fs)
	}

	return result, nil
}

// quoteStr returns a JSON-quoted string.
func quoteStr(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

// boolStr returns "true" or "false".
func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}
