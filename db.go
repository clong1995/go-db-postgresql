package db

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/clong1995/go-config"
	_ "github.com/lib/pq"
	"log"
	"reflect"
	"time"
)

var datasource *sql.DB

func init() {
	ds := config.Value("DATASOURCE")

	var err error
	datasource, err = sql.Open("postgres", ds)
	if err != nil {
		log.Fatalln(err)
	}

	datasource.SetMaxOpenConns(50)
	datasource.SetMaxIdleConns(10)
	datasource.SetConnMaxLifetime(time.Hour)
	if err = datasource.Ping(); err != nil {
		log.Println(err)
		return
	}
	log.Printf("[PostgreSQL] conn %s\n", ds)
}

// PrepareStmtTx 附带事物的预编译SQL批量执行
func PrepareStmtTx(query string, handle func(stmtTx *sql.Stmt) (err error)) (err error) {
	if err = Tx(func(tx *sql.Tx) (err error) {
		stmt, err := tx.Prepare(query)
		if err != nil {
			log.Println(err)
			return
		}
		defer func() {
			_ = stmt.Close()
		}()

		if err = handle(stmt); err != nil {
			log.Println(err)
			return
		}

		return
	}); err != nil {
		log.Println(err)
		return
	}
	return
}

// Tx 事物
func Tx(handle func(tx *sql.Tx) (err error)) (err error) {
	//开启事物
	tx, err := datasource.Begin()
	if err != nil {
		log.Println(err)
		return
	}

	defer func() {
		if err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				log.Println(rollbackErr)
			}
		} else {
			if commitErr := tx.Commit(); commitErr != nil {
				log.Println(commitErr)
			}
		}
	}()

	if err = handle(tx); err != nil {
		log.Println(err)
		return err
	}
	return
}

// QueryRow 查询一条
func QueryRow(query string, args ...any) (row *sql.Row) {
	row = datasource.QueryRow(query, args...)
	return
}

// Exec 执行
func Exec(query string, args ...any) (result sql.Result, err error) {
	if result, err = datasource.Exec(query, args...); err != nil {
		log.Println(err)
		return
	}
	return
}

// TxExec 事物内执行
func TxExec(tx *sql.Tx, query string, args ...any) (result sql.Result, err error) {
	if result, err = tx.Exec(query, args...); err != nil {
		log.Println(err)
		return
	}
	return
}

// Query 查询
func Query(query string, args ...any) (rows *sql.Rows, err error) {
	if rows, err = datasource.Query(query, args...); err != nil {
		log.Println(err)
		return
	}
	return
}

// QueryScan 查询并扫描
func QueryScan[T any](query string, args ...any) (res []T, err error) {
	rows, err := datasource.Query(query, args...)
	if err != nil {
		log.Println(err)
		return
	}
	defer func() {
		_ = rows.Close()
	}()

	if res, err = scan[T](rows); err != nil {
		log.Println(err)
		return
	}
	return
}

// TxQueryScan 事物内查询并扫描
func TxQueryScan[T any](tx *sql.Tx, query string, args ...any) (res []T, err error) {
	rows, err := tx.Query(query, args...)
	if err != nil {
		log.Println(err)
		return
	}
	defer func() {
		_ = rows.Close()
	}()

	if res, err = scan[T](rows); err != nil {
		log.Println(err)
		return
	}
	return
}

// TxQuery 事物内查询
func TxQuery(tx *sql.Tx, query string, args ...any) (rows *sql.Rows, err error) {
	rows, err = tx.Query(query, args...)
	if err != nil {
		log.Println(err)
		return
	}
	return
}

func scan[T any](rows *sql.Rows) (res []T, err error) {
	columns, err := rows.Columns()
	if err != nil {
		log.Println(err)
		return
	}

	var obj T
	objType := reflect.TypeOf(obj)

	if objType.Kind() == reflect.Struct {
		var fieldPointers []any
		var scanPointers []any
		var jsonPointers map[int]*[]byte

		if objType.NumField() != len(columns) {
			err = fmt.Errorf(`columns len = %d, objType len = %d`, len(columns), objType.NumField())
			log.Println(err)
			return
		}

		fieldPointers = make([]any, len(columns))
		jsonPointers = make(map[int]*[]byte)
		scanPointers = make([]any, len(columns))

		var field reflect.Value
		objValueElem := reflect.ValueOf(&obj).Elem()
		for i := range fieldPointers {
			field = objValueElem.Field(i)
			fieldPointers[i] = field.Addr().Interface()
			if field.Kind() == reflect.Struct || field.Kind() == reflect.Slice {
				var jsonData []byte
				jsonPointers[i] = &jsonData
				scanPointers[i] = jsonPointers[i]
			} else {
				scanPointers[i] = fieldPointers[i]
			}
		}
		for rows.Next() {
			if err = rows.Scan(scanPointers...); err != nil {
				log.Println(err)
				return
			}
			for k, v := range jsonPointers {
				if err = json.Unmarshal(*v, fieldPointers[k]); err != nil {
					log.Println(err)
					return
				}
			}
			res = append(res, obj)
		}
	} else {
		for rows.Next() {
			if err = rows.Scan(&obj); err != nil {
				log.Println(err)
				return
			}
			res = append(res, obj)
		}
	}

	if err = rows.Err(); err != nil {
		log.Println(err)
		return
	}

	return
}
