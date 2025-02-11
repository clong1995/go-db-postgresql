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
	objType := reflect.TypeOf(res)
	if objType.Kind() == reflect.Struct {
		length := objType.NumField()
		scanPointers := make([]any, length)
		objValueElem := reflect.ValueOf(&res).Elem()

		var field reflect.Value
		for i := 0; i < length; i++ {
			field = objValueElem.Field(i)
			scanPointers[i] = field.Addr().Interface()
		}

		if err = row.Scan(scanPointers...); err != nil {
			log.Println(err)
			return
		}
	} else {
		if err = row.Scan(&res); err != nil {
			log.Println(err)
			return
		}
	}

	//https://stackoverflow.com/questions/61704842/how-to-scan-a-queryrow-into-a-struct-with-pgx
	//res, err = pgx.CollectOneRow(row, pgx.RowToStructByPos[T])

	/*if res, err = pgx.RowToStructByPos[T](row); err != nil {
		log.Println(err)
		return
	}*/
	/*if err = row.Scan(); err != nil {
		log.Println(err)
		return
	}*/

	return
}
