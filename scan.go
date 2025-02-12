package db

import (
	"fmt"
	"github.com/jackc/pgx/v5"
	"log"
)

func scan[T any](rows pgx.Rows) (res []T, err error) {
	if res, err = pgx.CollectRows[T](rows, pgx.RowToStructByPos[T]); err != nil {
		fmt.Printf("CollectRows error: %v", err)
		return
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
