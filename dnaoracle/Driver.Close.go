package dnaoracle

func (drv *Driver) Close() error {
	return drv.db.Close()
}
