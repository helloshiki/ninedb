package engine

import "fmt"

var db DB

func init() {
	db.tables = make(map[string]*Table)
}

func (db *DB) getTable(tableName string) *Table {
	return db.tables[tableName]
}

func (db *DB) mustGetTable(tableName string) *Table {
	db.rwLock.RLock()
	if _, ok := db.tables[tableName]; !ok {
		panic(fmt.Errorf("table %s is not exsit", tableName))
	}
	db.rwLock.RUnlock()
	return db.tables[tableName]
}
