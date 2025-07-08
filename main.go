package main

import (
	"bufio"
	"fmt"
	"github.com/EternalVow/easyDB/parser"
	"github.com/EternalVow/easyDB/util"
	"net"
)

func main() {
	ln, err := net.Listen("tcp", ":3737")
	if err != nil {
		panic(err)
	}
	defer ln.Close()
	for {
		// 等待连接
		fmt.Println("等待连接...")
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		go handleConn(conn) // 每个连接一个 goroutine
	}
}

func handleConn(c net.Conn) {
	defer c.Close()
	scanner := bufio.NewScanner(c)
	scanner.Split(util.SplitBySemicolon) // 使用自定义分割函数
	for scanner.Scan() {
		line := scanner.Text()
		fmt.Println("scanner Received:", line)

		resp := parser.SqlParser(line)
		fmt.Println("scanner resp:", resp)

		c.Write([]byte("sql: " + line + "\n"))
		c.Write([]byte("resp: " + resp + "\n"))
	}
}
