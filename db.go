package pop

import (
	"context"
	"database/sql"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jmoiron/sqlx"
)

type dB struct {
	*sqlx.DB
	p           *pgxpool.Pool
	DialectName string
}

func (db *dB) SQLDB() *sql.DB {
	return db.DB.DB
}

func (db *dB) PGXPool() *pgxpool.Pool {
	return db.p
}

func (db *dB) TransactionContext(ctx context.Context) (*Tx, error) {
	return newTX(ctx, db, db.p, nil)
}

func (db *dB) Transaction() (*Tx, error) {
	return newTX(context.Background(), db, db.p, nil)
}

func (db *dB) TransactionContextOptions(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	return newTX(ctx, db, db.p, opts)
}

func (db *dB) Rollback() error {
	return nil
}

func (db *dB) Commit() error {
	return nil
}

func (db *dB) NamedQueryContext(ctx context.Context, query string, arg interface{}) (*sqlx.Rows, error) {
	if db.DialectName == NameYDB {
		q, args, err := NamedSetupYdb(query, arg)
		if err != nil {
			return nil, err
		}
		return db.DB.QueryxContext(ctx, q, args...)
	}
	return sqlx.NamedQueryContext(ctx, db, query, arg)
}

func (db *dB) NamedExecContext(ctx context.Context, query string, arg interface{}) (sql.Result, error) {
	//sqlx.Tx.NamedExecContext()
	if db.DialectName == NameYDB {
		q, args, err := NamedSetupYdb(query, arg)
		if err != nil {
			return nil, err
		}
		return db.DB.ExecContext(ctx, q, args...)
	}
	return sqlx.NamedExecContext(ctx, db, query, arg)
}

func (db *dB) QueryxContext(ctx context.Context, query string, args ...interface{}) (*sqlx.Rows, error) {
	if db.DialectName == NameYDB {
		q, as, err := SimpleSetup(query, args...)
		if err != nil {
			return nil, err
		}
		return db.DB.QueryxContext(ctx, q, as...)
	}
	return db.DB.QueryxContext(ctx, query, args...)
}

func (db *dB) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if db.DialectName == NameYDB {
		q, as, err := SimpleSetup(query, args...)
		if err != nil {
			return nil, err
		}
		return db.DB.QueryContext(ctx, q, as...)
	}
	return db.DB.QueryContext(ctx, query, args...)
}

func (db *dB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	if db.DialectName == NameYDB {
		q, as, err := SimpleSetup(query, args...)
		if err != nil {
			return nil, err
		}
		return db.DB.ExecContext(ctx, q, as...)
	}
	return db.DB.ExecContext(ctx, query, args...)
}
