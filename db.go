package db

import (
	"context"
	"github.com/clong1995/go-config"
	"github.com/jackc/pgx/v5/pgxpool"
	"log"
	"strconv"
)

var pool *pgxpool.Pool

func init() {
	ds := config.Value("DATASOURCE")

	conf, err := pgxpool.ParseConfig(ds)
	if err != nil {
		log.Fatalln(err)
	}

	num, err := strconv.ParseInt(config.Value("MAXCONNS"), 10, 32)
	if err != nil {
		log.Fatalln(err)
		return
	}

	conf.MaxConns = int32(num)
	if pool, err = pgxpool.NewWithConfig(context.Background(), conf); err != nil {
		log.Fatalln(err)
	}

	err = pool.Ping(context.Background())
	if err != nil {
		log.Fatalln(err)
	}

	log.Printf("[PostgreSQL] conn %s\n", ds)
}

func Close() {
	pool.Close()
	log.Println("db exited!")
}
