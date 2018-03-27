package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Llongfile)

	// basepath := "/dev/shm"
	basepath := "."

	dbpath := path.Join(basepath, "foo.db")

	os.Remove(dbpath)
	db, err := sql.Open("sqlite3", dbpath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	column_count := 50
	column_name := make([]string, 0)
	value_name := make([]string, 0)
	for i := 0; i < column_count; i++ {
		column_name = append(column_name, fmt.Sprintf("_%v", i))
		value_name = append(value_name, fmt.Sprintf("?"))
	}

	sqlStmt := fmt.Sprintf(`
	create table foo (id,name, %s );
	delete from foo;
	`, strings.Join(column_name, ","))
	_, err = db.Exec(sqlStmt)
	if err != nil {
		log.Printf("%q: %s\n", err, sqlStmt)
		return
	}

	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}
	cname := strings.Join(column_name, ",")
	cvalue := strings.Join(value_name, ",")

	psql := fmt.Sprintf("insert into foo(id, name, %s) values(?, ?, %s)", cname, cvalue)
	log.Printf("psql is %s", psql)
	stmt, err := tx.Prepare(psql)
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()
	insert_count := 50000
	insert_begin := time.Now()
	log.Printf("begin insert row count is %v", insert_count)
	for i := 0; i < insert_count; i++ {

		value_list := make([]interface{}, 0)
		for j := 0; j < column_count+2; j++ {
			value_list = append(value_list, fmt.Sprintf("value_%v_%v", i, j))
		}

		_, err = stmt.Exec(value_list[:]...)
		if err != nil {
			log.Fatal(err)
		}
	}
	tx.Commit()
	log.Printf("end insert row count is %v, use time is %v", insert_count, time.Since(insert_begin))

	rows, err := db.Query("select * from foo")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	count := 0
	query_time := time.Now()
	log.Printf("begin query row count is %v", count)
	for rows.Next() {
		value_list := make([]interface{}, 0)
		for j := 0; j < column_count+2; j++ {
			var v string
			value_list = append(value_list, &v)
		}
		err = rows.Scan(value_list...)
		if err != nil {
			log.Fatal(err)
		}
		if count == 0 {
			for i, v := range value_list {
				// mv := (*v).(string)
				log.Printf("i=%v,v=%s ", i, *(v.(*string)))
			}
		}
		count++
	}
	log.Printf("end query row count is %v, query use time is %v", count, time.Since(query_time))

	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}

	stmt, err = db.Prepare("select name from foo where id = ?")
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()
	var name string
	err = stmt.QueryRow("3").Scan(&name)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(name)

	_, err = db.Exec("delete from foo")
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec("insert into foo(id, name) values(1, 'foo'), (2, 'bar'), (3, 'baz')")
	if err != nil {
		log.Fatal(err)
	}

	rows, err = db.Query("select id, name from foo")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var id int
		var name string
		err = rows.Scan(&id, &name)
		if err != nil {
			log.Fatal(err)
		}
		log.Println(id, name)
	}
	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}
}
