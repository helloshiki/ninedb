package engine

import (
	"fmt"
	"log"
)

func (db *DB) GetTable(tableName string) *Table {
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

func (db *DB) CreateTable(row Row) {
	tableName := getTableName(row)
	log.Printf("tableName is %s", tableName)

	db.rwLock.Lock()
	defer db.rwLock.Unlock()

	if tb := db.GetTable(tableName); tb != nil {
		panic(fmt.Errorf("%s has been created", tableName))
	}

	db.tables[tableName] = newTable(tableName)
}

func (db *DB) Insert(row Row) error {
	return db.insert(row, false)
}

func (db *DB) Load(row Row) error {
	return db.insert(row, true)
}

func (db *DB) insert(row Row, isLoad bool) error {
	tableName := getTableName(row)
	table := db.mustGetTable(tableName)
	return table.insert(row, isLoad)
}

//全覆盖更新
func (db *DB) Update(row Row) error {
	tableName := getTableName(row)
	table := db.mustGetTable(tableName)
	return table.Update(row)
}

//更新某个列 cmd 支持REPLACE， INC, DESC
func (db *DB) UpdateFiled(row Row, fieldName string, cmd string, value interface{}) error {
	tableName := getTableName(row)
	table := db.mustGetTable(tableName)
	return table.UpdateFiled(row, fieldName, cmd, value)
}

func (db *DB) Get(row Row) Row {
	tableName := getTableName(row)
	table := db.mustGetTable(tableName)
	return table.Get(row)
}

func (db *DB) Delete(row Row) {
	tableName := getTableName(row)
	table := db.mustGetTable(tableName)
	table.Delete(row)
}

func newDB(dbName string) *DB {
	return &DB{
		dbName: dbName,
		tables: make(map[string]*Table),
	}
}
