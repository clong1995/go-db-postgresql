package db

import (
	"fmt"
	"github.com/jackc/pgx/v5"
	"log"
	"reflect"
)

func scan[T any](rows pgx.Rows) (res []T, err error) {
	var obj T
	typ := reflect.TypeOf(obj)
	//typ := reflect.TypeOf((*T)(nil)).Elem()
	if typ.Kind() == reflect.Struct {
		if res, err = pgx.CollectRows[T](rows, pgx.RowToStructByPos[T]); err != nil {
			fmt.Printf("CollectRows error: %v", err)
			return
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

	return
}

func scanOne[T any](rows pgx.Rows) (res T, err error) {
	if res, err = pgx.CollectOneRow[T](
		rows,
		pgx.RowToStructByPos[T],
	); err != nil {
		log.Fatal(err)
	}

	return
}
