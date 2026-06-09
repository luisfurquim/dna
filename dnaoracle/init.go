package dnaoracle

import (
	"github.com/luisfurquim/dna"
)

func init() {
	// Register Oracle-specific types in the dna type registry
	// so they can be reconstructed during deserialization of canonical JSON.
	dna.RegisterType(BigInt)
	dna.RegisterType(BigFloat)
	dna.RegisterType(Time)
}
