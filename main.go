package main

import (
	"flag"
	"github.com/shegaoyuan/ninedb/controller"
	"github.com/shegaoyuan/ninedb/engine"
	"github.com/shegaoyuan/ninedb/models"
	"github.com/shegaoyuan/ninedb/persist"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"time"
)

var (
	CpuProfile  = flag.String("cpu-profile", "", "write cpu profile to file")
	HeapProfile = flag.String("heap-profile", "", "write heap profile to file")
)

func main() {
	log.Printf("main")
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()
	if *CpuProfile != "" {
		file, err := os.Create(*CpuProfile)
		if err != nil {
			log.Panicln(err)
		}
		pprof.StartCPUProfile(file)
		defer pprof.StopCPUProfile()
	}

	if *HeapProfile != "" {
		file, err := os.Create(*HeapProfile)
		if err != nil {
			log.Panicln(err)
		}
		defer pprof.WriteHeapProfile(file)
	}
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	//for test
	sample()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)
	log.Println(<-sigChan)

}

func sample() {
	go persist.Work()
	//////////////////建表/////////////////////////////////////////
	u1 := models.User{UID: 1, GID: 0, TCC: 10000}
	engine.CreateTable(&u1)
	engine.Insert(&u1)
	engine.Insert(&u1)

	u2 := models.User{UID: 2, GID: 0, TCC: 10000}
	engine.Insert(&u2)

	m1 := models.TchMachine{}
	engine.CreateTable(&m1)
	//////////////////建表/////////////////////////////////////////

	///////////////////插入/////////////////////////////////////////
	cnt := 10
	start := time.Now().Unix()

	//插入cnt台矿机
	for i := 0; i < cnt; i++ {
		m := models.TchMachine{ID: i, GID: 0, UID: i % 10}
		//log.Printf("m:+%v", m)
		engine.Load(&m)
	}
	end := time.Now().Unix()
	log.Printf("insert %d records in %d second", cnt, end-start)
	///////////////////插入/////////////////////////////////////////

	///////////////////更新/////////////////////////////////////////
	start = time.Now().Unix()
	for i := 0; i < cnt; i++ {
		m := models.TchMachine{ID: i % 10, GID: 0, UID: i % 10}
		engine.Update(&m)
	}
	end = time.Now().Unix()
	log.Printf("update %d records in %d second", cnt, end-start)
	///////////////////更新/////////////////////////////////////////

	///////////////////转账/////////////////////////////////////////////
	//engine.UpdateFunc((controller.Transfer(nil, nil, nil)).(engine.CallBack))
	log.Printf("before transfer: user1: %+v, user2: %+v", engine.Get(&u1), engine.Get(&u2))

	controller.Transfer(1, 2, "TCC", 10)
	controller.Transfer(1, 2, "TCC", 100000000)

	log.Printf("after transfer: user1: %+v, user2: %+v", engine.Get(&u1), engine.Get(&u2))
	///////////////////转账/////////////////////////////////////////////

}
