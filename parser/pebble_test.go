package parser

import (
	"fmt"
	"github.com/cockroachdb/pebble"
	"github.com/xwb1989/sqlparser"
	"log"
	"testing"
)

func TestPebble(t *testing.T) {
	db, err := pebble.Open("demo", &pebble.Options{})
	if err != nil {
		log.Fatal(err)
	}
	key := []byte("hello")
	//if err := db.Set(key, []byte("world"), pebble.Sync); err != nil {
	//	log.Fatal(err)
	//}
	value, closer, err := db.Get(key)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s %s\n", key, value)
	if err := closer.Close(); err != nil {
		log.Fatal(err)
	}
	if err := db.Close(); err != nil {
		log.Fatal(err)
	}
}

func TestSqlParse(t *testing.T) {
	var sql string
	//sql = "create table lili (a int, b int, c int);"
	//sql = "insert into lili (a, b, c) values (1, 1, 1)"
	//sql = "DELETE  FROM lili WHERE a = 1"
	//sql = "SELECT * FROM lili WHERE a = 'abc'"
	sql = "update lili set b = 12 WHERE a = 1"
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		fmt.Println("解析失败:", err)
		// Do something with the err
	}
	det, ok := stmt.(*sqlparser.Update)
	if ok {
		fmt.Println(det)
	}
	fmt.Println(det)
}
