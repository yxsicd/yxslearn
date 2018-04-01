package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"
)

/*

function reqListener() {//   console.log(this.responseText);
}

function sendsql(db, sql) {
    var oReq = new XMLHttpRequest();
    oReq.onload = reqListener;
    sqle = encodeURIComponent(sql);
    urls = `?db=${db}&sql=${sqle}`
    oReq.open("get", urls, true);
    oReq.send();
}

for (var i = 0; i < 100000; i++) {
    sendsql("test.db", "select count(1) from people");
}



*/

var all_ret_len int64

func sendsql(db, sql string) {
	esql := url.QueryEscape(sql)
	edb := url.QueryEscape(db)
	ret, err := http.Get(fmt.Sprintf("http://localhost:8080/?db=%s&sql=%s", edb, esql))
	if err != nil {
		log.Printf("send sql err is %s", err)
		return
	}
	defer ret.Body.Close()
	retb, err := ioutil.ReadAll(ret.Body)
	if err != nil {
		log.Printf("sql run err is %s", err)
		return
	}
	all_ret_len = int64(len(retb)) + all_ret_len
	log.Printf("ret is %s", retb)
}

func batchsql(sql string, t_count int) {
	begin_time := time.Now()
	done := make(chan int, t_count)
	p_count := make(chan int, 256)
	for i := 0; i < t_count; i++ {
		go func() {
			p_count <- 1
			sendsql("test.db", sql)
			<-p_count
			done <- 1
		}()
	}
	for i := 0; i < t_count; i++ {
		<-done
	}

	use_time := time.Since(begin_time)
	speed := float64(t_count) / (float64(use_time.Nanoseconds()) / 1000000000)
	log.Printf("use time is %v, speed is %v, all_ret_content is %v, one requeset use time is %v",
		use_time, speed, all_ret_len/1024, use_time.Nanoseconds()/int64(t_count))
}

func main() {

	var batch_sql []string
	for i := 0; i < 1; i++ {
		batch_sql = append(batch_sql, "insert into people select 'n1','123123123';")
	}

	batchsql(strings.Join(batch_sql, " "), 5)
	batchsql("select count(1) from people;", 10)

}
