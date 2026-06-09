package dnaoci

import (
	"go/token"
)

var functions map[string]fnSpec = map[string]fnSpec{
	"abs":      {Func: nop, MinArgs: 1, MaxArgs: 1},
	"acos":     {Func: nop, MinArgs: 1, MaxArgs: 1},
	"asin":     {Func: nop, MinArgs: 1, MaxArgs: 1},
	"atan":     {Func: nop, MinArgs: 1, MaxArgs: 1},
	"ceil":     {Func: nop, MinArgs: 1, MaxArgs: 1},
	"cos":      {Func: nop, MinArgs: 1, MaxArgs: 1},
	"cosh":     {Func: nop, MinArgs: 1, MaxArgs: 1},
	"exp":      {Func: nop, MinArgs: 1, MaxArgs: 1},
	"floor":    {Func: nop, MinArgs: 1, MaxArgs: 1},
	"ln":       {Func: nop, MinArgs: 1, MaxArgs: 1},
	"log":      {Func: nop, MinArgs: 1, MaxArgs: 1},
	"mod":      {Func: nop, MinArgs: 2, MaxArgs: 2},
	"power":    {Func: nop, MinArgs: 2, MaxArgs: 2},
	"round":    {Func: nop, MinArgs: 1, MaxArgs: 2},
	"sign":     {Func: nop, MinArgs: 1, MaxArgs: 1},
	"sin":      {Func: nop, MinArgs: 1, MaxArgs: 1},
	"sinh":     {Func: nop, MinArgs: 1, MaxArgs: 1},
	"sqrt":     {Func: nop, MinArgs: 1, MaxArgs: 1},
	"tan":      {Func: nop, MinArgs: 1, MaxArgs: 1},
	"tanh":     {Func: nop, MinArgs: 1, MaxArgs: 1},
	"trunc":    {Func: nop, MinArgs: 1, MaxArgs: 1},
	"char":     {Func: nop, Ident: "chr", MinArgs: 1, MaxArgs: 1},
	"concat":   {Func: nop, MinArgs: 2, MaxArgs: 2},
	"lower":    {Func: nop, MinArgs: 1, MaxArgs: 1},
	"ltrim":    {Func: nop, MinArgs: 1, MaxArgs: 2},
	"replace":  {Func: nop, MinArgs: 3, MaxArgs: 3},
	"rtrim":    {Func: nop, MinArgs: 1, MaxArgs: 2},
	"soundex":  {Func: nop, MinArgs: 1, MaxArgs: 1},
	"substr":   {Func: nop, MinArgs: 2, MaxArgs: 3},
	"trim":     {Func: nop, MinArgs: 1, MaxArgs: 2},
	"upper":    {Func: nop, MinArgs: 1, MaxArgs: 1},
	"len":      {Func: nop, Ident: "length", MinArgs: 1, MaxArgs: 1},
	"bytelen":  {Func: nop, Ident: "lengthb", MinArgs: 1, MaxArgs: 1},
	"now":      {Func: now, MinArgs: 1, MaxArgs: 1},
	"date":     {Ident: "date", MinArgs: 0, MaxArgs: 0},
	"max":      {Func: nop, Ident: "greatest", MinArgs: 2, MaxArgs: -1},
	"min":      {Func: nop, Ident: "least", MinArgs: 2, MaxArgs: -1},
	"coalesce": {Func: nop, MinArgs: 2, MaxArgs: -1},
	"iif":      {Func: iif, MinArgs: 3, MaxArgs: 3},
	"avg":      {Func: nop, MinArgs: 1, MaxArgs: 1},
	"count":    {Func: nop, MinArgs: 1, MaxArgs: 1},
	"sum":      {Func: nop, MinArgs: 1, MaxArgs: 1},
}

var binop map[token.Token]string = map[token.Token]string{
	token.ADD:     "+",
	token.SUB:     "-",
	token.MUL:     "*",
	token.QUO:     "/",
	token.EQL:     "=",
	token.NEQ:     "!=",
	token.GTR:     ">",
	token.LSS:     "<",
	token.GEQ:     ">=",
	token.LEQ:     "<=",
	token.LAND:    " AND ",
	token.LOR:     " OR ",
	token.REM:     " LIKE ",
	token.AND:     "&",
	token.OR:      "|",
	token.AND_NOT: " AND NOT ",
	token.SHL:     "<<",
	token.SHR:     ">>",
}

var unop map[token.Token]string = map[token.Token]string{
	token.ADD:   "+",
	token.SUB:   "-",
	token.XOR:   "~",
	token.ARROW: ":",
	token.NOT:   " NOT ",
}

func nop(ident string, args []string) (string, []string, error) {
	return ident, args, nil
}

func iif(ident string, args []string) (string, []string, error) {
	return " case " + args[0] + " then " + args[1] + " else " + args[2] + " end ", nil, nil
}

func date(ident string, args []string) (string, []string, error) {
	return ident, args, nil
}

func now(ident string, args []string) (string, []string, error) {
	return ident, args, nil
}
