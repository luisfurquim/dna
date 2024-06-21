package dnaoracle

func (drv *Driver) Exists(tabName string) bool {
	var ok bool
	_, ok = drv.find[tabName]
	return ok
}

