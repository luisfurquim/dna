package dnaoci

func (drv *Driver) Exists(tabName string) bool {
	_, ok := drv.find[tabName]
	return ok
}
