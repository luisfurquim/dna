package dnaoci

import (
	"github.com/luisfurquim/dna"
)

func init() {
	dna.RegisterType(BigInt)
	dna.RegisterType(BigFloat)
	dna.RegisterType(Time)
}
