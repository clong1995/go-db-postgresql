package db

import (
	"github.com/jackc/pgx/v5"
	"log"
	"reflect"
)

func scan[T any](rows pgx.Rows) (res []T, err error) {
	var obj T
	objType := reflect.TypeOf(obj)
	//查询多行，无法使用RowsToStructsByXxx，因为查询结果数据量巨大，同理，查询量巨大的，也不应该用本方法。
	//rows.Next为流式，比如在写入csv、xlsx，推送的时候，就可以只处理当前条，节省内存。
	if objType.Kind() == reflect.Struct {
		length := objType.NumField()
		scanPointers := make([]any, length)
		objValueElem := reflect.ValueOf(&obj).Elem()

		var field reflect.Value
		for i := 0; i < length; i++ {
			field = objValueElem.Field(i)
			scanPointers[i] = field.Addr().Interface()
		}

		for rows.Next() {
			if err = rows.Scan(scanPointers...); err != nil {
				log.Println(err)
				return
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

func scanOne[T any](row pgx.Row) (res T, err error) {
	if err = row.Scan(&res); err != nil {
		log.Println(err)
		return
	}
	return
}
