package dnasqlite

func (drv *Driver) Update(tabName string, parms ...interface{}) error {
	return drv.update[tabName]["id"].Exec(parms...)
}

	
