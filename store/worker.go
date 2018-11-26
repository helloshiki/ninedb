package store

import (
	"github.com/shegaoyuan/ninedb/engine"
	//"encoding/json"
	"log"
	"time"
)

const (
	WORKCNT = 1
)

func work() {
	for i := 0; i < WORKCNT; i++ {
		go func() {
			ticker := time.NewTicker(3 * time.Second)
			counter := 0
			printedCounter := 0
			for {
				select {
				case trx := <-engine.ReqChan:
					switch trx.Cmd {
					case "INSERT", "UPDATE":
						//log.Printf("insert/update record %s %d", trx.TableName, trx.ID)
						lastest, buf := engine.GetData(trx)
						var resp *engine.Response
						if buf != nil {
							//todo save to db
							time.Sleep(100 * time.Millisecond) //模拟数据库插入耗时

							counter++

							resp = &engine.Response{Code: "OK", TableName: trx.TableName, ID: trx.ID, SavedVersion: lastest, SavedStamp: time.Now().Unix()}
						} else {
							resp = &engine.Response{Code: "SKIP", TableName: trx.TableName, ID: trx.ID, SavedVersion: trx.Version, SavedStamp: time.Now().Unix()}
						}
						engine.PutResp(resp)

					case "DELETE":
						log.Printf("delete record %d", trx.ID)
						resp := &engine.Response{Code: "OK", TableName: trx.TableName, ID: trx.ID, SavedVersion: trx.Version, SavedStamp: time.Now().Unix()}
						engine.PutResp(resp)

					}
				case <-ticker.C:
					if counter != printedCounter {
						log.Printf("=======update db %d times======", counter)
						printedCounter = counter
					}
				}
			}
		}()

	}
}

func init() {
	work()
}
