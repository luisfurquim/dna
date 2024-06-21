package dnasqlite

import (
	"go/token"
)

var functions map[string]fnSpec = map[string]fnSpec{
	"abs": fnSpec{
		Func: nop,
		MinArgs: 1,
		MaxArgs: 1,
	},
	"acos": fnSpec{
      Func: nop,
		MinArgs: 1,
		MaxArgs: 1,
	},
	"asin": fnSpec{
      Func: nop,
		MinArgs: 1,
		MaxArgs: 1,
	},
	"atan": fnSpec{
      Func: nop,
		MinArgs: 1,
		MaxArgs: 1,
	},
	"ceil": fnSpec{
      Func: nop,
		MinArgs: 1,
		MaxArgs: 1,
	},
	"cos": fnSpec{
      Func: nop,
		MinArgs: 1,
		MaxArgs: 1,
	},
	"cosh": fnSpec{
      Func: nop,
		MinArgs: 1,
		MaxArgs: 1,
	},
	"exp": fnSpec{
      Func: nop,
		MinArgs: 1,
		MaxArgs: 1,
	},
	"floor": fnSpec{
      Func: nop,
		MinArgs: 1,
		MaxArgs: 1,
	},
	"ln": fnSpec{
      Func: nop,
		MinArgs: 1,
		MaxArgs: 1,
	},
	"log": fnSpec{
      Func: nop,
		MinArgs: 1,
		MaxArgs: 1,
	},
	"mod": fnSpec{
      Func: nop,
		MinArgs: 2,
		MaxArgs: 2,
	},
	"power": fnSpec{
      Func: nop,
		MinArgs: 2,
		MaxArgs: 2,
	},
	"round": fnSpec{
      Func: nop,
		MinArgs: 1,
		MaxArgs: 2,
	},
	"sign": fnSpec{
      Func: nop,
		MinArgs: 1,
		MaxArgs: 1,
	},
	"sin": fnSpec{
      Func: nop,
		MinArgs: 1,
		MaxArgs: 1,
	},
	"sinh": fnSpec{
      Func: nop,
		MinArgs: 1,
		MaxArgs: 1,
	},
	"sqrt": fnSpec{
      Func: nop,
		MinArgs: 1,
		MaxArgs: 1,
	},
	"tan": fnSpec{
      Func: nop,
		MinArgs: 1,
		MaxArgs: 1,
	},
	"tanh": fnSpec{
      Func: nop,
		MinArgs: 1,
		MaxArgs: 1,
	},
	"trunc": fnSpec{
      Func: nop,
		MinArgs: 1,
		MaxArgs: 1,
	},
	"char": fnSpec{
      Func: nop,
		MinArgs: 1,
		MaxArgs: 1,
	},
	"concat": fnSpec{
      Func: nop,
		MinArgs: 2,
		MaxArgs: 2,
	},
	"lower": fnSpec{
      Func: nop,
		MinArgs: 1,
		MaxArgs: 1,
	},
	"ltrim": fnSpec{
      Func: nop,
		MinArgs: 1,
		MaxArgs: 2,
	},
	"replace": fnSpec{
      Func: nop,
		MinArgs: 3,
		MaxArgs: 3,
	},
	"rtrim": fnSpec{
      Func: nop,
		MinArgs: 1,
		MaxArgs: 2,
	},
	"soundex": fnSpec{
      Func: nop,
		MinArgs: 1,
		MaxArgs: 1,
	},
	"substr": fnSpec{
      Func: nop,
		MinArgs: 2,
		MaxArgs: 3,
	},
	"trim": fnSpec{
      Func: nop,
		MinArgs: 1,
		MaxArgs: 2,
	},
	"upper": fnSpec{
      Func: nop,
		MinArgs: 1,
		MaxArgs: 1,
	},
	"len": fnSpec{
      Func: nop,
      Ident: "length",
		MinArgs: 1,
		MaxArgs: 1,
	},
	"bytelen": fnSpec{
      Func: nop,
      Ident: "octet_length",
		MinArgs: 1,
		MaxArgs: 1,
	},
	"now": fnSpec{
      Func: now,
		MinArgs: 1,
		MaxArgs: 1,
	},
	"date": fnSpec{
      Ident: "date",
		MinArgs: 0,
		MaxArgs: 0,
	},

/*
	"date": fnSpec{
      Ident: "date",
		MinArgs: 0,
		MaxArgs: 0,
	},
	"datetime": fnSpec{
      Ident: "datetime",
		MinArgs: 0,
		MaxArgs: 0,
	},
	"time": fnSpec{
      Ident: "time",
		MinArgs: 0,
		MaxArgs: 0,
	},
*/

	"max": fnSpec{
      Func: nop,
		MinArgs: 2,
		MaxArgs: -1,
	},
	"min": fnSpec{
      Func: nop,
		MinArgs: 2,
		MaxArgs: -1,
	},
	"coalesce": fnSpec{
      Func: nop,
		MinArgs: 2,
		MaxArgs: -1,
	},
	"iif": fnSpec{
      Func: nop,
		MinArgs: 3,
		MaxArgs: 3,
	},
	"avg": fnSpec{
      Func: nop,
		MinArgs: 1,
		MaxArgs: 1,
	},
	"count": fnSpec{
      Func: nop,
		MinArgs: 1,
		MaxArgs: 1,
	},
	"sum": fnSpec{
      Func: nop,
		MinArgs: 1,
		MaxArgs: 1,
	},
}

var binop map[token.Token]string = map[token.Token]string{
	token.ADD:		"+",
	token.SUB:		"-",
	token.MUL:		"*",
	token.QUO:		"/",
	token.EQL:		"=",
	token.NEQ:		"!=",
	token.GTR:		">",
	token.LSS:		"<",
	token.GEQ:		">=",
	token.LEQ:		"<=",
	token.LAND:		" AND ",
	token.LOR:		" OR ",
	token.REM:		" LIKE ",
	token.AND:		"&",
	token.OR:		"|",
	token.AND_NOT: " AND NOT ",
	token.SHL:		"<<",
	token.SHR:		">>",
}

var unop map[token.Token]string = map[token.Token]string{
	token.ADD:		"+",
	token.SUB:		"-",
	token.XOR:		"~",
	token.ARROW:	":",
	token.NOT:		" NOT ",
}

var tmFmt map[string]string = map[string]string{
	"d":  "%e",
	"0d": "%d",
	"D":  "ltrim(strftime('%j',$$),'0')",
	"0D": "%j",
	"7d": "%u",
	
}

func nop(ident string, args []string) (string, []string, error) {
	return ident, args, nil
}

func date(ident string, args []string) (string, []string, error) {
	
	return ident, args, nil
}

func now(ident string, args []string) (string, []string, error) {
	
	return ident, args, nil
}
