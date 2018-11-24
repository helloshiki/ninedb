package engine

import (
	"encoding/json"
	"log"
	"time"
)

type Transaction struct {
	Cmd       string
	TableName string
	ID        int
	Version   uint64
}

type Response struct {
	Code         string
	TableName    string
	ID           int
	SavedVersion uint64
	SavedStamp   int64
}

func putTrx(trx *Transaction) {
	select {
	case ReqChan <- trx:
	default:
		log.Printf("reqChain is full")
	}
}

func GetTrx() *Transaction {
	return <-ReqChan
}

func PutResp(res *Response) {
	select {
	case RespChan <- res:
	default:
		log.Printf("respChain is full")
	}
}

func getResp() *Response {
	return <-RespChan
}

func work() {
	go func() {
		for {
			resp := getResp()
			switch resp.Code {
			case "OK":
				updateSavedVersion(resp)
				log.Printf("table %s's record %d, version %d done at %d", resp.TableName, resp.ID, resp.SavedVersion, resp.SavedStamp)
			case "SKIP":
				//log.Printf("table %s's record %d, version %d skipped at %d", resp.TableName, resp.ID, resp.SavedVersion, resp.SavedStamp)
			}
		}
	}()
}

func GetData(trx *Transaction) (uint64, []byte) { //return latest version data
	lock := db.locks[trx.TableName]
	lock.Lock()
	defer lock.Unlock()

	meta, ok := db.metas[trx.TableName][trx.ID]
	if !ok || meta.SavedVersion >= trx.Version { //记录已被删除或当前版本小于已保存版本
		return 0, nil
	}

	rid := db.rows[trx.TableName][trx.ID]
	obj := db.tables[trx.TableName][rid]
	ver := meta.Version
	buf, _ := json.Marshal(obj)

	return ver, buf
}

func updateSavedVersion(resp *Response) {
	lock := db.locks[resp.TableName]
	lock.Lock()
	defer lock.Unlock()

	if meta, ok := db.metas[resp.TableName][resp.ID]; ok && meta.SavedVersion < resp.SavedVersion {
		meta.SavedVersion = resp.SavedVersion
		meta.SavedStamp = time.Now().Unix()
	}
}

var ReqChan chan *Transaction
var RespChan chan *Response

func init() {
	ReqChan = make(chan *Transaction, 100*M)
	RespChan = make(chan *Response, 100*M)
	work()
}
