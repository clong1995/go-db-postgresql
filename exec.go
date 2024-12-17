package db

import (
	"context"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"log"
)

// PrepareStmtTx 附带事物的预编译SQL批量执行
func PrepareStmtTx(query string, handle func(stmtTx string) (err error)) (err error) {
	/*if err = Tx(func(tx pgx.Tx) (err error) {
		stmtName := "stmt"
		stmt, err := tx.Prepare(context.Background(), stmtName, query)
		if err != nil {
			log.Println(err)
			return
		}
		defer func() {
			pool.
		}()

		if err = handle(stmt); err != nil {
			log.Println(err)
			return
		}

		return
	}); err != nil {
		log.Println(err)
		return
	}*/
	return
}

// Tx 事物
func Tx(handle func(tx pgx.Tx) (err error)) (err error) {
	//开启事物
	tx, err := pool.Begin(context.Background())
	if err != nil {
		log.Println(err)
		return
	}

	defer func() {
		if err != nil {
			if rollbackErr := tx.Rollback(context.Background()); rollbackErr != nil {
				log.Println(rollbackErr)
			}
		} else {
			if commitErr := tx.Commit(context.Background()); commitErr != nil {
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
func QueryRow(query string, args ...any) (row pgx.Row) {
	row = pool.QueryRow(context.Background(), query, args...)
	return
}

// Exec 执行
func Exec(query string, args ...any) (result pgconn.CommandTag, err error) {
	if result, err = pool.Exec(context.Background(), query, args...); err != nil {
		log.Println(err)
		return
	}
	return
}

// TxExec 事物内执行
func TxExec(tx pgx.Tx, query string, args ...any) (result pgconn.CommandTag, err error) {
	if result, err = tx.Exec(context.Background(), query, args...); err != nil {
		log.Println(err)
		return
	}
	return
}

// Query 查询
func Query(query string, args ...any) (rows pgx.Rows, err error) {
	if rows, err = pool.Query(context.Background(), query, args...); err != nil {
		log.Println(err)
		return
	}
	return
}

// QueryScan 查询并扫描
func QueryScan[T any](query string, args ...any) (res []T, err error) {
	rows, err := pool.Query(context.Background(), query, args...)
	if err != nil {
		log.Println(err)
		return
	}
	defer rows.Close()

	if res, err = scan[T](rows); err != nil {
		log.Println(err)
		return
	}
	return
}

// TxQueryScan 事物内查询并扫描
func TxQueryScan[T any](tx pgx.Tx, query string, args ...any) (res []T, err error) {
	rows, err := tx.Query(context.Background(), query, args...)
	if err != nil {
		log.Println(err)
		return
	}
	defer rows.Close()

	if res, err = scan[T](rows); err != nil {
		log.Println(err)
		return
	}
	return
}

// TxQuery 事物内查询
func TxQuery(tx pgx.Tx, query string, args ...any) (rows pgx.Rows, err error) {
	rows, err = tx.Query(context.Background(), query, args...)
	if err != nil {
		log.Println(err)
		return
	}
	return
}