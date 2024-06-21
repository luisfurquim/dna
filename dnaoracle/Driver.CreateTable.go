package dnaoracle


import (
	"fmt"
	"reflect"
	"strings"
   "strconv"
	"github.com/luisfurquim/dna"
)

func (drv *Driver) CreateTable(tabName string, fieldSpecs []dna.FieldSpec) error {
	var err error
	var colNames string
	var fieldSpec dna.FieldSpec
	var prec, frac int
	var pkConstraint string

	for _, fieldSpec = range fieldSpecs {
		if fieldSpec.JoinList {
			continue
		}
		if len(colNames) > 0 {
			colNames += ","
		}
		colNames += `"` + fieldSpec.Name + `"`
		if fieldSpec.PK {
			colNames += " NUMBER"
			if len(fieldSpec.Prec) >=1 {
				colNames += "(" + fieldSpec.Prec[0] + ")"
				fmt.Sscanf(fieldSpec.Prec[0], "%d", &prec)
			} else {
				prec = 38
			}
			colNames += fmt.Sprintf(" GENERATED ALWAYS AS IDENTITY INCREMENT BY 1 START WITH 1 MINVALUE 1 MAXVALUE %s NOCYCLE NOT NULL", strings.Repeat("9", prec))
			pkConstraint = `, CONSTRAINT "` + tabName + `_PK" PRIMARY KEY ("` + fieldSpec.Name + `")`
		} else if len(fieldSpec.Fk) > 0 { // needs FK prec
			colNames += " NUMBER"
		} else {
			switch fieldSpec.Type.Kind() {
			case reflect.Bool:
				colNames += ` CHAR(1 BYTE)`
			case reflect.Int8, reflect.Uint8:
				if len(fieldSpec.Prec) >=1 {
					fmt.Sscanf(fieldSpec.Prec[0], "%d", &prec)
					if prec>3 {
						prec = 3
					}
				} else {
					prec = 3
				}

				colNames += fmt.Sprintf(` NUMBER(%d)`, prec)

			case reflect.Int16, reflect.Uint16:
				if len(fieldSpec.Prec) >=1 {
					fmt.Sscanf(fieldSpec.Prec[0], "%d", &prec)
					if prec>5 {
						prec = 5
					}
				} else {
					prec = 5
				}

				colNames += fmt.Sprintf(` NUMBER(%d)`, prec)
					
			case reflect.Int32, reflect.Uint32:
				if len(fieldSpec.Prec) >=1 {
					fmt.Sscanf(fieldSpec.Prec[0], "%d", &prec)
					if prec>10 {
						prec = 10
					}
				} else {
					prec = 10
				}

				colNames += fmt.Sprintf(` NUMBER(%d)`, prec)
					
			case reflect.Int, reflect.Uint:
				if strconv.IntSize == 32 {
					if len(fieldSpec.Prec) >=1 {
						fmt.Sscanf(fieldSpec.Prec[0], "%d", &prec)
						if prec>10 {
							prec = 10
						}
					} else {
						prec = 10
					}
				} else {
					if len(fieldSpec.Prec) >=1 {
						fmt.Sscanf(fieldSpec.Prec[0], "%d", &prec)
						if prec>19 {
							prec = 19
						}
					} else {
						prec = 19
					}
				}

				colNames += fmt.Sprintf(` NUMBER(%d)`, prec)

			case reflect.Int64, reflect.Uint64:
				if len(fieldSpec.Prec) >=1 {
					fmt.Sscanf(fieldSpec.Prec[0], "%d", &prec)
					if prec>19 {
						prec = 19
					}
				} else {
					prec = 19
				}

				colNames += fmt.Sprintf(` NUMBER(%d)`, prec)

			case reflect.Float32:
				colNames += fmt.Sprintf(` BINARY_FLOAT`)
				
			case reflect.Float64:
				colNames += fmt.Sprintf(` BINARY_DOUBLE`)
				
			case reflect.Struct:
				if fieldSpec.Type == BigInt {
					if len(fieldSpec.Prec) >=1 {
						fmt.Sscanf(fieldSpec.Prec[0], "%d", &prec)
						if prec>38 {
							prec = 38
						}
					} else {
						prec = 38
					}

					colNames += fmt.Sprintf(` NUMBER(%d)`, prec)
				} else if fieldSpec.Type == BigFloat {
					if len(fieldSpec.Prec) >=2 {
						fmt.Sscanf(fieldSpec.Prec[1], "%d", &frac)
						if frac>34 {
							frac = 34
						}
					} else {
						frac = 34
					}
					if len(fieldSpec.Prec) >=1 {
						fmt.Sscanf(fieldSpec.Prec[0], "%d", &prec)
						if prec>38 {
							prec = 38
						}
					} else {
						prec = 38
					}

					colNames += fmt.Sprintf(` NUMBER(%d,%d)`, prec, frac)

				} else if fieldSpec.Type == Time {
					if len(fieldSpec.Prec) >=1 {
						fmt.Sscanf(fieldSpec.Prec[0], "%d", &prec)
						if prec>9 {
							prec = 9
						}
					} else {
						prec = 9
					}

					colNames += fmt.Sprintf(` TIMESTAMP (%d) WITH TIME ZONE`, prec)
				}

			case reflect.Array, reflect.Slice:
				colNames += ` BLOB`


			case reflect.String:
				if len(fieldSpec.Prec) >=1 {
					fmt.Sscanf(fieldSpec.Prec[0], "%d", &prec)
				} else {
					return ErrSyntax
				}

				if prec < longString {
					colNames += fmt.Sprintf(` NVARCHAR2(%d)`, prec)
				} else {
					colNames += fmt.Sprintf(` NCLOB`)
				}

			default:
				Goose.Init.Logf(1,"%#v: %s", fieldSpec, ErrUnsupportedType)
			
				return ErrUnsupportedType
			}

		}
	}

	Goose.Init.Logf(5,`CREATE TABLE "%s" (%s%s)`, tabName, colNames, pkConstraint)

	_, err = drv.db.Exec(fmt.Sprintf(`CREATE TABLE "%s" (%s%s)`, tabName, colNames, pkConstraint))
	if err != nil {
		if strings.HasPrefix(fmt.Sprintf("%s",err), "ORA-00955") {
			return nil
		}
		Goose.Init.Logf(1,"Error creating %s table: %s", tabName, err)
	}

	return err
}
