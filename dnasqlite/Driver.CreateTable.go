package dnasqlite

import (
	"fmt"
	"github.com/luisfurquim/dna"
)

func (drv *Driver) CreateTable(tabName string, fieldSpecs []dna.FieldSpec) error {
	var err error
	var colNames string
	var fieldSpec dna.FieldSpec

	for _, fieldSpec = range fieldSpecs {
		if fieldSpec.JoinList {
			continue
		}
		if len(colNames) > 0 {
			colNames += ","
		}
		colNames += "`" + fieldSpec.Name + "`"
		if fieldSpec.PK {
			colNames += " INTEGER PRIMARY KEY"
		}
	}

	err = drv.db.Exec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s` (%s)", tabName, colNames))
	if err != nil {
		Goose.Init.Logf(1,"Error creating %s table: %s", tabName, err)
	}

	return err
}
