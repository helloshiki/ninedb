package engine

import (
	"fmt"
	"log"
	"reflect"
	"sort"
	"sync"
	"time"
)

func getTableName(row Row) string {
	val := reflect.ValueOf(row)
	typ := reflect.Indirect(val).Type()
	tableName := typ.Name()

	if val.Kind() != reflect.Ptr {
		panic(fmt.Errorf("cannot use non-ptr struct %s", tableName))
	}

	return tableName
}

func CreateTable(row Row) {
	tableName := getTableName(row)
	log.Printf("tableName is %s", tableName)

	db.rwLock.Lock()
	defer db.rwLock.Unlock()

	if tb := db.getTable(tableName); tb != nil {
		panic(fmt.Errorf("%s has been created", tableName))
	}

	db.tables[tableName] = newTable(tableName)
}

func newTable(tableName string) *Table {
	return &Table{
		tableName:  tableName,
		rows:       make([]Row, 0),
		idxIndexes: make(map[int]int),
		metas:      make(map[int]*MetaInfo),
		indexes:    make(map[string][]int),
		sorting:    make(map[string]bool),
		sortlock:   &sync.Mutex{},
		lock:       &sync.Mutex{},
		allocChan:  make(chan int, ROWSIZE),
	}
}

////////////////////////////内部调用////////////////////////////////////////////////////////
func (tb *Table) nextIdx() int {
	select {
	case index := <-tb.allocChan:
		return index
	default:
		allocSize := len(tb.rows)
		toAppend := make([]Row, ROWSIZE/2)
		tb.rows = append(tb.rows, toAppend...)
		for i := 0; i < ROWSIZE/2; i++ {
			tb.allocChan <- allocSize + i
		}
		return <-tb.allocChan
	}
}

func put(tableName string, rid int) {
	//check table exist and lock outer
	select {
	case db.getTable(tableName).allocChan <- rid:
		return
	default:
		log.Printf("table %s's chan is full", tableName)
	}
}

func (table *Table) sortIndex(index string) {
	slock := table.sortlock
	slock.Lock()
	if table.sorting[index] {
		slock.Unlock()
		return
	}
	table.sorting[index] = true
	slock.Unlock()

	time.AfterFunc(2*time.Second, func() {
		slock := table.sortlock
		slock.Lock()
		table.sorting[index] = false
		slock.Unlock()

		start := time.Now().Unix()

		lock := table.lock
		lock.Lock()
		indexes := table.indexes
		length := len(indexes[index])
		sort.IntSlice(indexes[index]).Sort()
		lock.Unlock()

		end := time.Now().Unix()
		log.Printf("sort index %s:%s %d records finished in %d second", table.tableName, index, length, end-start)
	})
}

////////////////////////////////////////////////////////////////////////////////////

func Insert(row Row) error {
	return insert(row, false)
}

func Load(row Row) error {
	return insert(row, true)
}

func insert(row Row, isLoad bool) error {
	tableName := getTableName(row)
	table := db.mustGetTable(tableName)
	uid := row.GetUID()

	lock := table.lock
	lock.Lock()
	defer lock.Unlock()

	if _, ok := table.idxIndexes[uid]; ok { //exist
		rid := table.idxIndexes[uid]
		log.Printf("record id[%d] is exist in table %s %d row", uid, tableName, rid)
		return fmt.Errorf("record %d is exist in %s", uid, tableName)
	}

	idx := table.nextIdx()

	//创建meta
	meta := &MetaInfo{Version: 1, UpdateStamp: time.Now().Unix(), SavedVersion: 0}
	////从数据库加载时load需传值，避免回写
	if isLoad {
		meta.SavedVersion = 1
	}
	table.metas[uid] = meta

	table.rows[idx] = row
	table.idxIndexes[uid] = idx

	//发起持久化指令
	putTrx(&Transaction{Cmd: "INSERT", TableName: tableName, ID: uid, Version: meta.Version})

	//添加到主键列表
	pk := PRIMARYKEY
	table.indexes[pk] = append(table.indexes[pk], uid)
	//列表排序
	table.sortIndex(pk)

	//log.Printf("insert record id[%d] in table %s's %d row", id, tableName, rid)

	indexs := row.Index()
	if indexs == nil {
		return nil
	}

	//存在索引，创建索引
	val := reflect.ValueOf(row)
	for i := 0; i < len(indexs); i++ {
		if len(indexs[i]) == 0 {
			continue
		}
		pk := tableName
		sort.StringSlice(indexs[i]).Sort()
		for j := 0; j < len(indexs[i]); j++ {
			pk += fmt.Sprintf(":%s:%v", indexs[i][j], reflect.Indirect(val).FieldByName(indexs[i][j]))
		}
		table.indexes[pk] = append(table.indexes[pk], uid)
		//索引排序
		table.sortIndex(pk)
	}
	return nil
}

//全覆盖更新
func Update(row Row) error {
	tableName := getTableName(row)
	table := db.mustGetTable(tableName)

	uid := row.GetUID()

	lock := table.lock
	lock.Lock()
	defer lock.Unlock()

	if rid, ok := table.idxIndexes[uid]; ok {
		table.rows[rid] = row
		//更新meta
		meta := table.metas[uid]
		meta.Version += 1
		meta.UpdateStamp = time.Now().Unix()

		//发起持久化指令
		putTrx(&Transaction{Cmd: "UPDATE", TableName: tableName, ID: uid, Version: meta.Version})

		//log.Printf("update record id[%d] in table %s's %d row", id, tableName, rid)

	} else {
		log.Printf("record %d is not exist in table %s", uid, tableName)
		return fmt.Errorf("record %d is not exist in table %s", uid, tableName)
	}
	return nil
}

//更新某个列 cmd 支持REPLACE， INC, DESC
func UpdateFiled(row Row, fieldName string, cmd string, value interface{}) error {
	tableName := getTableName(row)
	table := db.mustGetTable(tableName)
	uid := row.GetUID()

	lock := table.lock
	lock.Lock()
	defer lock.Unlock()

	if rid, ok := table.idxIndexes[uid]; ok {
		val := reflect.ValueOf(table.rows[rid]).Elem()
		switch val.FieldByName(fieldName).Type().Kind() {
		case reflect.String:
			val.FieldByName(fieldName).SetString(value.(string))
		case reflect.Int64, reflect.Int32, reflect.Int:
			switch cmd {
			case "REPLACE":
				val.FieldByName(fieldName).SetInt(value.(int64))
			case "INC":
				val.FieldByName(fieldName).SetInt(val.FieldByName(fieldName).Int() + value.(int64))
			case "DESC":
				if val.FieldByName(fieldName).Int() >= value.(int64) {
					val.FieldByName(fieldName).SetInt(val.FieldByName(fieldName).Int() - value.(int64))
				} else {
					return fmt.Errorf("record %d %s not enough", uid, fieldName)
				}
			default:
				panic(fmt.Errorf("unsupport update cmd %s ", cmd))
			}
		default:
			fmt.Printf("type is %+v", val.FieldByName(fieldName).Type().Kind())
		}
		//更新meta
		meta := table.metas[uid]
		meta.Version += 1
		meta.UpdateStamp = time.Now().Unix()

		//发起持久化指令
		putTrx(&Transaction{Cmd: "UPDATE", TableName: tableName, ID: uid, Version: meta.Version})

		//log.Printf("update record id[%d] in table %s's %d row", id, tableName, rid)

	} else {
		log.Printf("record %d is not exist in table %s", uid, tableName)
		return fmt.Errorf("record %d is not exist in table %s", uid, tableName)
	}
	return nil
}

func Get(row Row) Row {
	val := reflect.ValueOf(row)
	typ := reflect.Indirect(val).Type()
	tableName := typ.Name()

	table := db.mustGetTable(tableName)

	uid := row.GetUID()
	lock := table.lock
	lock.Lock()
	defer lock.Unlock()

	if rid, ok := table.idxIndexes[uid]; ok {
		return table.rows[rid]
	}

	log.Printf("record %d is not exist in table %s", uid, tableName)
	return nil
}

func Delete(row Row) {
	tableName := getTableName(row)
	table := db.mustGetTable(tableName)
	uid := row.GetUID()
	lock := table.lock
	lock.Lock()
	defer lock.Unlock()

	rid, ok := table.idxIndexes[uid]
	if !ok {
		return
	}

	meta := table.metas[uid]
	delete(table.idxIndexes, uid)
	delete(table.metas, uid)
	put(tableName, rid)

	//发起持久化指令
	putTrx(&Transaction{Cmd: "DELETE", TableName: tableName, ID: uid, Version: meta.Version})

	//删除主键列表
	pk := PRIMARYKEY
	for k := 0; k < len(table.indexes[pk]); k++ {
		if table.indexes[pk][k] == uid {
			table.indexes[pk][k] = table.indexes[pk][len(table.indexes[pk])-1]
			table.indexes[pk] = table.indexes[pk][:len(table.indexes[pk])-1]
		}
	}
	//列表排序
	table.sortIndex(pk)

	log.Printf("delete recoed %d from %s", uid, tableName)

	indexs := row.Index()
	if indexs == nil {
		return
	}

	//存在索引，删除索引
	val := reflect.ValueOf(row)
	for i := 0; i < len(indexs); i++ {
		if len(indexs[i]) == 0 {
			continue
		}
		pk := tableName
		sort.StringSlice(indexs[i]).Sort()
		for j := 0; j < len(indexs[i]); j++ {
			pk += fmt.Sprintf(":%s:%v", indexs[i][j], reflect.Indirect(val).FieldByName(indexs[i][j]))
		}
		for k := 0; k < len(table.indexes[pk]); k++ {
			if table.indexes[pk][k] == uid {
				table.indexes[pk][k] = table.indexes[pk][len(table.indexes[pk])-1]
				table.indexes[pk] = table.indexes[pk][:len(table.indexes[pk])-1]
			}
		}
		//索引排序
		table.sortIndex(pk)
	}
	//log.Printf("index is %+v", db.indexs[tableName])
}
