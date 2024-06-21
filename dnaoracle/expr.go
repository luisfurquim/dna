package dnaoracle

import (
	"fmt"
	"go/ast"
	"strings"
	"go/token"
	"go/parser"
)

func xlate(e ast.Expr) (string, error) {
	var s, s1, s2 string
	var err error
	var n int
	var i int64
	var ok bool
	var sargs []string
	var fun fnSpec
	var ident *ast.Ident
	var sident string
	var lit *ast.BasicLit

	switch el := e.(type) {
	case *ast.BinaryExpr:
		if s, ok = binop[el.Op]; !ok {
			return "", ErrSyntax
		}

		s1, err = xlate(el.X)
		if err != nil {
			return "", err
		}

		s2, err = xlate(el.Y)
		if err != nil {
			return "", err
		}

		if s=="=" && strings.ToUpper(strings.Trim(s2," ")) == "NULL" {
			return s1 + " IS NULL ", nil
		}

		if s=="!=" && strings.ToUpper(strings.Trim(s2," ")) == "NULL" {
			return s1 + " IS NOT NULL ", nil
		}

		return s1 + s + s2, nil

	case *ast.UnaryExpr:
		if el.Op == token.ARROW {
			return ":" + el.X.(*ast.Ident).Name, nil
		}
		
		if s, ok = unop[el.Op]; !ok {
			return "", ErrSyntax
		}

		s1, err = xlate(el.X)
		if err != nil {
			return "", err
		}

		return s + s1, nil

	case *ast.IndexListExpr:
		if len(el.Indices) != 2 {
			return "", ErrSyntax
		}

		s, err = xlate(el.X)
		if err != nil {
			return "", err
		}

		s1, err = xlate(el.Indices[0])
		if err != nil {
			return "", err
		}

		s2, err = xlate(el.Indices[1])
		if err != nil {
			return "", err
		}

		return " " + s + " BETWEEN " + s1 + " AND " + s2 + " ", nil

	case *ast.ParenExpr:
		s, err = xlate(el.X)
		return "(" + s + ") ", err

	case *ast.CallExpr:
		if ident, ok = el.Fun.(*ast.Ident); !ok {
			if lit, ok = el.Fun.(*ast.BasicLit); !ok {
				return "", ErrSyntax
			}
		}

		for n=0; n<len(el.Args); n++ {
			s1, err = xlate(el.Args[n])
			if err != nil {
				return "", err
			}
			sargs = append(sargs, s1)
		}

		if lit == nil {
			if fun, ok = functions[ident.Name]; ok {
				if len(el.Args)<fun.MinArgs || len(el.Args)>fun.MaxArgs {
					return "", ErrSyntax
				}

				if fun.Ident != "" {
					sident, sargs, err = fun.Func(fun.Ident, sargs)
				} else {
					sident, sargs, err = fun.Func(ident.Name, sargs)
				}
				if sargs == nil {
					return " " + sident + " ", nil
				}
				return " " + sident + "(" + strings.Join(sargs,",") + ") ", nil
			}
			return ` "` + ident.Name + `" IN (` + strings.Join(sargs,",") + ") ", nil
		}

		if lit.Kind == token.STRING {
			if lit.Value[0]=='`' {
				return " " + lit.Value[1:len(lit.Value)-1] + " IN (" + strings.Join(sargs,",") + ") ", nil
			}
			return " '" + lit.Value[1:len(lit.Value)-1] + "' IN (" + strings.Join(sargs,",") + ") ", nil
		}

		return " " + lit.Value + " IN (" + strings.Join(sargs,",") + ") ", nil

	case *ast.Ident:
		if strings.ToUpper(el.Name) == "NULL" {
			return ` NULL`, nil
		}
		return ` "` + el.Name + `"`, nil

	case *ast.BasicLit:
		switch el.Kind {
		case token.INT:
			if len(el.Value)>=2 && el.Value[:2] == "0x" {
				fmt.Sscanf(el.Value[2:],"%x",&i)
			} else if len(el.Value)>=2 && el.Value[:2] == "0o" {
				fmt.Sscanf(el.Value[2:],"%o",&i)
			} else if el.Value[0] == '0' {
				fmt.Sscanf(el.Value[1:],"%o",&i)
			} else {
				fmt.Sscanf(el.Value,"%d",&i)
			}
			return fmt.Sprintf("%d",i), nil
		case token.STRING:
			return "'" + el.Value[1:len(el.Value)-1] + "'", nil
		case token.CHAR, token.FLOAT:
			return el.Value, nil
		default:
			return "", ErrSyntax
		}
	}

	return "", ErrSyntax
}

func expr(e string) (string, error) {
	var err error
	var exp ast.Expr

	exp, err = parser.ParseExpr(strings.Replace(e,"'",`"`,-1))
	if err != nil {
		Goose.Init.Logf(1, "Error parsing expression `%s`: %s", e, err)
		return "", err
	}

	return xlate(exp)
}
