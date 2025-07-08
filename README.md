# easyDB

## 介绍
easyDB是一个基于rocksdb的数据库管理系统，是一个简单的数据库管理系统，支持以下功能：
## 功能
- [yes] 连接数据库
- [yes] 插入数据
- [yes] 删除数据
- [yes] 更新数据
- [yes] 查询数据

## 使用方式

```bash

// 编译
go build -o easyDB
./easyDB

```


```bash



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

```

## 架构介绍

**三个层次：**
- 网络层：使用tcp协议，使用go语言net包，主线程accept，子线程处理连接
- 引擎层：引擎利用了sqlparser转换成ast结构，通过不同的类型执行不同的操作，目前支持create table, insert, select, update, delete
- 存储层：使用rocksdb实现，采用了pebble 库，是go版本的rocksdb


## todo
- [ ] 目前只支持int类型，需要支持其他类型
- [ ] 目前没有索引和事务，需要支持索引和事务
