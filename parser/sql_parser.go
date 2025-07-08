package parser

import (
	"errors"
	"fmt"
	"github.com/EternalVow/easyDB/util"
	"github.com/cockroachdb/pebble"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/xwb1989/sqlparser"
	"log"
	"regexp"
	"strconv"
	"strings"
)

var OK = "ok"

func SqlParser(sql string) string {
	//sql := "SELECT * FROM table WHERE a = 'abc'"
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		fmt.Println("解析失败:", err)
		return ""
		// Do something with the err
	}

	// Otherwise do something with stmt
	switch stmt := stmt.(type) {
	case *sqlparser.Select:
		//_ = stmt
		fmt.Printf("%+v \n", stmt)
		headers, rows, err := Select(stmt)
		if err != nil {
			log.Fatal(err)
			return err.Error()
		}

		// 将结果转换为表格
		t := table.Table{}
		headerRow := table.Row{}
		for _, h := range headers {
			headerRow = append(headerRow, h)
		}
		t.AppendHeader(headerRow)

		for _, row := range rows {
			rowData := table.Row{}
			for _, byteItem := range row {
				// todo 返回需要制定类型，才知道这么转换
				//目前都是数字
				// 尝试将数字转字符

				intStr := strconv.Itoa(int(byteItem))

				//intStr := uint8(byteItem) + 48
				rowData = append(rowData, string(intStr))
			}
			t.AppendRow(rowData)
		}
		// 打印表格
		t.Render()
		return t.Render()
		break

	case *sqlparser.Insert:
		fmt.Printf("%+v \n", stmt)
		descT, err := InsertInto(stmt)
		if err != nil {
			log.Fatal(err)
			return err.Error()
		}
		return descT
		break
	case *sqlparser.Update:
		fmt.Printf("%+v \n", stmt)
		descT, err := Update(stmt)
		if err != nil {
			log.Fatal(err)
			return err.Error()
		}
		return descT
		break
	case *sqlparser.Delete:
		descT, err := Del(stmt)
		if err != nil {
			log.Fatal(err)
			return err.Error()
		}
		return descT
		break
	case *sqlparser.DDL:
		fmt.Printf("%+v \n", stmt)
		// 创建表
		// 建立一个 pebble 数据库
		// 然后里面一个key 对应的schema
		// 然后里面一个key 对应的ddl 语句
		err := CreateTable(stmt, sql)
		if err != nil {
			return err.Error()
		}
		return OK
		break
	//case *sqlparser.Show:
	//	fmt.Printf("%+v \n", stmt)
	//	// 创建表
	//	// 建立一个 pebble 数据库
	//	// 然后里面一个key 对应的schema
	//	// 然后里面一个key 对应的ddl 语句
	//	DescTable(stmt)
	//	break
	default:
		descT, table, err := extractDesc(sql)
		if err != nil {
			fmt.Println(err)
			break
		}
		if descT {
			desc, err := DescTable(&sqlparser.Show{
				OnTable: sqlparser.TableName{
					Name: sqlparser.NewTableIdent(table),
				},
			})

			if err != nil {
				fmt.Println(err)
				break
			}
			fmt.Println("DescTable desc", desc)
			return desc
		}

		//fmt.Println("Unknown statement type")
	}

	return ""

}

func Select(stmt *sqlparser.Select) ([]string, [][]byte, error) {
	if len(stmt.From) != 1 {
		return nil, nil, errors.New("not support table nums")
	}
	table := stmt.From[0]
	aliasedTableExpr, ok := table.(*sqlparser.AliasedTableExpr)
	if !ok {
		return nil, nil, errors.New("not support AliasedTableExpr")
	}
	tableName, ok := aliasedTableExpr.Expr.(sqlparser.TableName)
	if !ok {
		return nil, nil, errors.New("not support tableName")
	}

	db, err := pebble.Open(tableName.Name.String(), &pebble.Options{})
	if err != nil {
		log.Fatal(err)
	}
	defer func(db *pebble.DB) {
		err := db.Close()
		if err != nil {
			log.Fatal(err)
		}
	}(db)

	// 写入数据集 按a 为主键 key
	var keys = make(map[string]bool) // key => true

	if stmt.Where.Expr != nil {
		// 解析 where 条件
		// 只支持 =
		// 只支持一个条件
		// 只支持一个字段
		// 只支持一个值

		comparisonExpr, ok := stmt.Where.Expr.(*sqlparser.ComparisonExpr)
		if !ok {
			return nil, nil, errors.New("not support ComparisonExpr")
		}

		if comparisonExpr.Operator != sqlparser.EqualStr {
			return nil, nil, errors.New("not support Operator")
		}
		if comparisonExpr.Right == nil {
			return nil, nil, errors.New("not support Right Expr")
		}
		key, ok := comparisonExpr.Right.(*sqlparser.SQLVal)
		if !ok {
			return nil, nil, errors.New("not support Right val")
		}

		keys[string(key.Val)] = true

	}

	keyMap := make(map[string][]byte)
	for key, _ := range keys {
		fmt.Printf("get key %+v \n", key)
		value, err := util.DBGet(db, key)
		if err != nil {
			log.Fatal(err)
			return nil, nil, err
		}

		keyMap[key] = value
		strvalue := string(value)
		fmt.Printf("select %s %s\n", key, strvalue)
	}

	// 序列化
	schema, err := util.DBGet(db, "schema")
	if err != nil {
		log.Fatal(err)
		return nil, nil, err
	}

	// 解析 schema
	headers := strings.Split(string(schema), ",")
	// 遍历 keys
	var result = make([][]byte, 0, len(headers))
	for _, c := range keyMap {
		newc := make([]byte, len(c))
		copy(newc, c)
		result = append(result, newc)
	}
	return headers, result, nil
}

func extractDesc(input string) (bool, string, error) {
	// 定义正则表达式，匹配 desc 后面的单词
	re := regexp.MustCompile(`^desc\s+(\w+)$`)
	matches := re.FindStringSubmatch(input)
	if len(matches) < 2 {
		return false, "", fmt.Errorf("no match found")
	}
	// 返回捕获组中的内容，即 lili
	return true, matches[1], nil
}

func CreateTable(stmt *sqlparser.DDL, sql string) error {
	if stmt.Action != sqlparser.CreateStr {
		return fmt.Errorf("not a create table statement")
	}

	db, err := pebble.Open(stmt.NewName.Name.String(), &pebble.Options{})
	if err != nil {
		log.Fatal(err)
		return err
	}

	defer func(db *pebble.DB) {
		err := db.Close()
		if err != nil {
			log.Fatal(err)
		}
	}(db)

	ddlkey := []byte("ddl")

	value, closer, _ := db.Get(ddlkey)
	if string(value) != "" {
		log.Println("Get:", "已经存在表", string(value))
		if err := closer.Close(); err != nil {
			log.Fatal(err)
			return err
		}
		return nil
	}
	fmt.Printf("CreateTable %s %s\n", ddlkey, value)

	if err := db.Set(ddlkey, []byte(sql), pebble.Sync); err != nil {
		log.Fatal(err)
	}

	var headers []string
	for _, column := range stmt.TableSpec.Columns {
		headers = append(headers, column.Name.String())
		//fmt.Println(column.Name.String(), column.Type)
	}
	schemakey := []byte("schema")
	// 进行 JSON 序列化
	ColumnsData := strings.Join(headers, ",")
	log.Printf("create table %s %s\n", schemakey, ColumnsData)
	// 写入数据集 按a 为主键 key
	if err := db.Set(schemakey, []byte(ColumnsData), pebble.Sync); err != nil {
		log.Fatal("set schemakey", err)
	}
	value, closer, err = db.Get(schemakey)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("CreateTable %s %s\n", schemakey, value)
	if err := closer.Close(); err != nil {
		log.Fatal(err)
	}

	return nil
}

func DescTable(stmt *sqlparser.Show) (string, error) {

	db, err := pebble.Open(stmt.OnTable.Name.String(), &pebble.Options{})
	if err != nil {
		log.Fatal(err)
	}
	defer func(db *pebble.DB) {
		err := db.Close()
		if err != nil {
			log.Fatal(err)
		}
	}(db)
	ddlkey := []byte("ddl")

	value, closer, err := db.Get(ddlkey)
	if err != nil {
		log.Fatal(err)
	}

	ddlvalue := string(value)
	fmt.Printf("DescTable %s %s\n", ddlkey, ddlvalue)
	if err := closer.Close(); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("DescTable return  %s %s\n", ddlkey, ddlvalue)
	return string(ddlvalue), err
}

func InsertInto(stmt *sqlparser.Insert) (string, error) {

	db, err := pebble.Open(stmt.Table.Name.String(), &pebble.Options{})
	if err != nil {
		log.Fatal(err)
		return "", err
	}
	defer func(db *pebble.DB) {
		err := db.Close()
		if err != nil {
			log.Fatal(err)
		}
	}(db)
	ddlkey := []byte("ddl")

	value, closer, err := db.Get(ddlkey)
	if err != nil {
		log.Fatal(err)
		return "", err
	}

	// todo ddl 需要做校验
	ddlvalue := string(value)
	fmt.Printf("InsertInto %s %s\n", ddlkey, ddlvalue)
	if err := closer.Close(); err != nil {
		log.Fatal(err)
		return "", err
	}

	// 写入数据集 按a 为主键 key
	//var columnsMap = make(map[string]string) // columns => value
	//for index, column := range stmt.Columns {
	//	//
	//
	//	columnsMap[column.String()] = values[index].
	//}

	values, ok := stmt.Rows.(sqlparser.Values)
	if !ok {
		return "", errors.New("values failed")
	}

	//rocksMapArr := make([]map[string][]byte, 0, len(values))
	rocksMap := make(map[string][]byte)

	// 每一行的值
	for _, rowVal := range values {
		//实际写入的只，key 主键，[]byte 实际的内容
		var key string
		var rocksVal = make([]byte, 0)
		// 每个colunm
		for indexVal, colunmVal := range rowVal {
			v, ok := colunmVal.(*sqlparser.SQLVal)
			if !ok {
				return "", errors.New("values failed")
			}
			//buf:=sqlparser.TrackedBuffer
			if indexVal == 0 {
				key = string(v.Val)
			}
			switch v.Type {
			case sqlparser.StrVal:
				rocksVal = append(rocksVal, v.Val...)
			case sqlparser.IntVal:
				// 转成int
				intVal, err := strconv.Atoi(string(v.Val))
				if err != nil {
					return "", err
				}
				rocksVal = append(rocksVal, byte(intVal))
			}

		}
		rocksMap[key] = rocksVal
		//rocksMapArr = append(rocksMapArr, rocksMap)
	}

	// 写入数据
	for key, rocks := range rocksMap {
		fmt.Printf("InsertInto row %+v %+v\n", key, rocks)

		if err := db.Set([]byte(key), []byte(rocks), pebble.Sync); err != nil {
			log.Fatal(err)
			return "", err
		}
	}

	return OK, err
}

func Del(stmt *sqlparser.Delete) (string, error) {
	if len(stmt.TableExprs) != 1 {
		return "", errors.New("not support table nums")
	}
	table := stmt.TableExprs[0]
	aliasedTableExpr, ok := table.(*sqlparser.AliasedTableExpr)
	if !ok {
		return "", errors.New("not support AliasedTableExpr")
	}
	tableName, ok := aliasedTableExpr.Expr.(sqlparser.TableName)
	if !ok {
		return "", errors.New("not support tableName")
	}
	db, err := pebble.Open(tableName.Name.String(), &pebble.Options{})
	if err != nil {
		log.Fatal(err)
	}
	defer func(db *pebble.DB) {
		err := db.Close()
		if err != nil {
			log.Fatal(err)
		}
	}(db)

	// 写入数据集 按a 为主键 key
	var keys = make(map[string]bool) // key => true

	if stmt.Where.Expr != nil {
		// 解析 where 条件
		// 只支持 =
		// 只支持一个条件
		// 只支持一个字段
		// 只支持一个值

		comparisonExpr, ok := stmt.Where.Expr.(*sqlparser.ComparisonExpr)
		if !ok {
			return "", errors.New("not support ComparisonExpr")
		}

		if comparisonExpr.Operator != sqlparser.EqualStr {
			return "", errors.New("not support Operator")
		}
		if comparisonExpr.Right == nil {
			return "", errors.New("not support Right Expr")
		}
		key, ok := comparisonExpr.Right.(*sqlparser.SQLVal)
		if !ok {
			return "", errors.New("not support Right val")
		}

		keys[string(key.Val)] = true

	}

	for key, _ := range keys {
		fmt.Printf("delete row %+v \n", key)
		if err := db.Delete([]byte(key), pebble.Sync); err != nil {
			log.Fatal(err)
			return "", err
		}
	}
	return OK, nil
}

func Update(stmt *sqlparser.Update) (string, error) {
	if len(stmt.TableExprs) != 1 {
		return "", errors.New("not support table nums")
	}
	table := stmt.TableExprs[0]
	aliasedTableExpr, ok := table.(*sqlparser.AliasedTableExpr)
	if !ok {
		return "", errors.New("not support AliasedTableExpr")
	}
	tableName, ok := aliasedTableExpr.Expr.(sqlparser.TableName)
	if !ok {
		return "", errors.New("not support tableName")
	}
	db, err := pebble.Open(tableName.Name.String(), &pebble.Options{})
	if err != nil {
		log.Fatal(err)
	}
	defer func(db *pebble.DB) {
		err := db.Close()
		if err != nil {
			log.Fatal(err)
		}
	}(db)

	// 写入数据集 按a 为主键 key
	var keys = make(map[string]bool) // key => true

	if stmt.Where.Expr != nil {
		// 解析 where 条件
		// 只支持 =
		// 只支持一个条件
		// 只支持一个字段
		// 只支持一个值

		comparisonExpr, ok := stmt.Where.Expr.(*sqlparser.ComparisonExpr)
		if !ok {
			return "", errors.New("not support ComparisonExpr")
		}

		if comparisonExpr.Operator != sqlparser.EqualStr {
			return "", errors.New("not support Operator")
		}
		if comparisonExpr.Right == nil {
			return "", errors.New("not support Right Expr")
		}
		key, ok := comparisonExpr.Right.(*sqlparser.SQLVal)
		if !ok {
			return "", errors.New("not support Right val")
		}

		keys[string(key.Val)] = true

	}

	// 写入数据集 按a 为主键 key
	var setKeyMap = make(map[string][]byte) // key => true
	//
	if stmt.Exprs != nil {
		for _, expr := range stmt.Exprs {
			key := expr.Name.Name.String()
			val, ok := expr.Expr.(*sqlparser.SQLVal)
			if !ok {
				return "", errors.New("not support SQLVal")
			}
			value := val.Val

			switch val.Type {
			case sqlparser.IntVal:
				intval, err := strconv.Atoi(string(value))
				if err != nil {
					log.Fatal(err)
					return "", err
				}
				setKeyMap[key] = []byte{byte(intval)} // 只支持一个值

			case sqlparser.StrVal:
			default:
				return "", errors.New("not support SQLVal")

			}
		}

	}

	for key, _ := range keys {

		schema, err := util.DBGet(db, "schema")
		if err != nil {
			log.Fatal(err)
			return "", err
		}

		originVal, err := util.DBGet(db, key)
		if err != nil {
			log.Fatal(err)
			return "", err
		}

		// 解析 schema
		// 找到 key 的位置
		// 然后更新 value
		//var updateByte = make([]byte, len(schema))
		headers := strings.Split(string(schema), ",")
		for hi, h := range headers {
			if _, ok := setKeyMap[h]; ok {
				originVal[hi] = setKeyMap[h][0]
			}
		}

		fmt.Printf("update row %+v \n", key)
		if err := db.Set([]byte(key), originVal, pebble.Sync); err != nil {
			log.Fatal(err)
			return "", err
		}
	}
	return OK, nil
}
