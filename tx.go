package pop

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/gobuffalo/pop/v6/internal/randx"
	"github.com/jackc/pgx/v5/pgxpool"
	"math/rand"
	"time"

	"github.com/jmoiron/sqlx"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// Tx stores a transaction with an ID to keep track.
type Tx struct {
	ID int
	*sqlx.Tx
	db          *sql.DB
	pool        *pgxpool.Pool
	DialectName string
}

func newTX(ctx context.Context, db *dB, pool *pgxpool.Pool, opts *sql.TxOptions) (*Tx, error) {

	t := &Tx{
		ID:          randx.NonNegativeInt(),
		db:          db.SQLDB(),
		pool:        pool,
		DialectName: db.DialectName,
	}
	tx, err := db.BeginTxx(ctx, opts)
	t.Tx = tx
	if err != nil {
		return nil, fmt.Errorf("could not create new transaction: %w", err)
	}
	return t, nil
}

func (tx *Tx) SQLDB() *sql.DB {
	return tx.db
}

func (tx *Tx) PGXPool() *pgxpool.Pool {
	return tx.pool
}

func (tx *Tx) PingContext(ctx context.Context) error {
	return tx.db.PingContext(ctx)
}

// TransactionContext simply returns the current transaction,
// this is defined so it implements the `Store` interface.
func (tx *Tx) TransactionContext(ctx context.Context) (*Tx, error) {
	return tx, nil
}

// TransactionContextOptions simply returns the current transaction,
// this is defined so it implements the `Store` interface.
func (tx *Tx) TransactionContextOptions(_ context.Context, _ *sql.TxOptions) (*Tx, error) {
	return tx, nil
}

// Transaction simply returns the current transaction,
// this is defined so it implements the `Store` interface.
func (tx *Tx) Transaction() (*Tx, error) {
	return tx, nil
}

// Close does nothing. This is defined so it implements the `Store` interface.
func (tx *Tx) Close() error {
	return nil
}

// Workaround for https://github.com/jmoiron/sqlx/issues/447
func (tx *Tx) NamedQueryContext(ctx context.Context, query string, arg interface{}) (*sqlx.Rows, error) {
	if tx.DialectName == NameYDB {
		q, args, err := NamedSetupYdb(query, arg)
		if err != nil {
			return nil, err
		}
		return tx.Tx.QueryxContext(ctx, q, args...)
	}
	return sqlx.NamedQueryContext(ctx, tx, query, arg)
}

func (tx *Tx) NamedExecContext(ctx context.Context, query string, arg interface{}) (sql.Result, error) {
	if tx.DialectName == NameYDB {
		q, args, err := NamedSetupYdb(query, arg)
		if err != nil {
			return nil, err
		}
		return tx.Tx.ExecContext(ctx, q, args...)
	}
	return sqlx.NamedExecContext(ctx, tx, query, arg)
}

func (tx *Tx) QueryxContext(ctx context.Context, query string, args ...interface{}) (*sqlx.Rows, error) {
	if tx.DialectName == NameYDB {
		q, as, err := SimpleSetup(query, args...)
		if err != nil {
			return nil, err
		}
		return tx.Tx.QueryxContext(ctx, q, as...)
	}
	return tx.Tx.QueryxContext(ctx, query, args...)
}

func (tx *Tx) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	if tx.DialectName == NameYDB {
		q, as, err := SimpleSetup(query, args...)
		if err != nil {
			return nil, err
		}
		return tx.Tx.ExecContext(ctx, q, as...)
	}
	return tx.Tx.ExecContext(ctx, query, args...)
}

func (tx *Tx) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if tx.DialectName == NameYDB {
		q, as, err := SimpleSetup(query, args...)
		if err != nil {
			return nil, err
		}
		return tx.Tx.QueryContext(ctx, q, as...)
	}
	return tx.Tx.QueryContext(ctx, query, args...)
}
