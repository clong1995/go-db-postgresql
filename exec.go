package db

import (
	"context"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"log"
)

/*type Key struct {
	key string
}*/

// PrepareStmtTx 附带事物的预编译SQL批量执行
func PrepareStmtTx(stmtName, query string, handle func(stmtTx string) (err error)) (err error) {
	if err = Tx(func(tx pgx.Tx) (err error) {
		if _, err = tx.Prepare(context.Background(), stmtName, query); err != nil {
			log.Println(err)
			return
		}
		defer func() {
			if err = tx.Conn().Deallocate(context.Background(), stmtName); err != nil {
				log.Println(err)
				return
			}
		}()
		if err = handle(stmtName); err != nil {
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

// BatchTx 批量数据的插入
func BatchTx(tx pgx.Tx, query string, data [][]any) (err error) {
	batch := &pgx.Batch{}
	for _, v := range data {
		_ = batch.Queue(query, v...)
	}
	br := tx.SendBatch(context.Background(), batch)
	if err = br.Close(); err != nil {
		log.Println(err)
		return
	}
	return
}

// CopyTx 超大量数据插入的
func CopyTx(tx pgx.Tx, tableName string, columnNames []string, data [][]any) {
	table := pgx.Identifier{tableName}
	_, err := tx.CopyFrom(
		context.Background(),
		table,
		columnNames,
		pgx.CopyFromRows(data),
	)
	if err != nil {
		log.Println(err)
		return
	}
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

// QueryRowScan 查询并扫描
func QueryRowScan[T any](query string, args ...any) (res T, err error) {
	row := pool.QueryRow(context.Background(), query, args...)
	if res, err = scanOne[T](row); err != nil {
		log.Println(err)
		return
	}
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

// TxQueryRow 事物内查询
func TxQueryRow(tx pgx.Tx, query string, args ...any) (row pgx.Row) {
	row = tx.QueryRow(context.Background(), query, args...)
	return
}
