package db

import (
	"github.com/jackc/pgx/v5"
	"log"
	"reflect"
)

func scan[T any](rows pgx.Rows) (res []T, err error) {
	var obj T
	objType := reflect.TypeOf(obj)

	if objType.Kind() == reflect.Struct {

		length := objType.NumField()

		scanPointers := make([]any, length)
		var field reflect.Value
		objValueElem := reflect.ValueOf(&obj).Elem()

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
