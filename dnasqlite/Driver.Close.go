package dnasqlite

func (drv *Driver) Close() error {
	return drv.db.Close()
}
