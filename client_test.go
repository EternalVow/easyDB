package main

import (
	"fmt"
	"log"
	"net"
	"testing"
	"time"
)

func TestClient(t *testing.T) {
	// 连接服务器
	conn, err := net.Dial("tcp", "127.0.0.1:3737")
	if err != nil {
		fmt.Println("连接失败:", err)
		return
	}
	defer conn.Close()
	fmt.Println("连接成功，开始发送数据...")

	sendSql(conn, "create table lili (a int, b int, c int);")

	sendSql(conn, "desc lili;")

	sendSql(conn, "insert into lili (a, b, c) values (1, 1, 1);")

	sendSql(conn, "SELECT * FROM lili WHERE a = 1;")

	sendSql(conn, "update lili set b = 12 WHERE a = 1;")

	sendSql(conn, "SELECT * FROM lili WHERE a = 1;")

	sendSql(conn, "DELETE  FROM lili WHERE a = 1;")

	sendSql(conn, "SELECT * FROM lili WHERE a = 1;")

	//
	//
	//createTable := "create table lili (a int, b int, c int);"
	//_, err = conn.Write([]byte(createTable))
	//if err != nil {
	//	fmt.Println("发送失败:", err)
	//	return
	//}
	//fmt.Printf("已发送: %s \n", createTable)
	//
	//time.Sleep(1 * time.Second) // 每秒发送一次，可根据需求调整
	//
	//// 读取服务器响应
	//msg := make([]byte, 1024)
	//lenc, _ := conn.Read([]byte(msg))
	//fmt.Printf("服务器响应:\n %s \n", msg[0:lenc])
	//
	//descTable := "desc lili;"
	//_, err = conn.Write([]byte(descTable))
	//if err != nil {
	//	fmt.Println("发送失败:", err)
	//	return
	//}
	//fmt.Printf("已发送: %s \n", descTable)
	//
	//time.Sleep(1 * time.Second) // 每秒发送一次，可根据需求调整
	//
	//// 读取服务器响应
	//msg = make([]byte, 1024)
	//lenc, _ = conn.Read([]byte(msg))
	//fmt.Printf("服务器响应:\n %s \n", msg[0:lenc])
	//
	////持续发送数据
	//for i := 0; ; i++ {
	//	msg := fmt.Sprintf("select * from lili where a =  %d;", i)
	//	_, err := conn.Write([]byte(msg))
	//	if err != nil {
	//		fmt.Println("发送失败:", err)
	//		return
	//	}
	//	fmt.Printf("已发送: %s \n", msg)
	//	time.Sleep(1 * time.Second) // 每秒发送一次，可根据需求调整
	//	newmsg := make([]byte, 1024)
	//	lenc, _ = conn.Read([]byte(newmsg))
	//	fmt.Printf("服务器响应:\n %s \n", newmsg[0:lenc])
	//
	//}
}

func sendSql(conn net.Conn, sql string) {
	//createTable := "create table lili (a int, b int, c int);"
	_, err := conn.Write([]byte(sql))
	if err != nil {
		fmt.Println("发送失败:", err)
		return
	}
	fmt.Printf("已发送: %s \n", sql)

	time.Sleep(1 * time.Second) // 每秒发送一次，可根据需求调整

	msg := make([]byte, 1024)
	lenc, _ := conn.Read([]byte(msg))
	log.Println("服务器响应:")
	log.Printf("%s \n", msg[0:lenc])
	//fmt.Printf("服务器响应:\n %s \n", msg[0:lenc])
}
