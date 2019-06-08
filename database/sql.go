package database

import (
	"context"
	"database/sql"
	"sync/atomic"
	// _ "github.com/go-sql-driver/mysql"
)

//主从数据库
type MRDB struct {
	master *conn
	reads  []*conn
	idx    int64
}

type conn struct {
	db  *sql.DB
	dsn string
}

type MRDBConfig struct {
	DN        string
	MasterDSN string
	ReadDSNs  []string
}

func NewMRDB(cfg *MRDBConfig) *MRDB {
	mrDB := &MRDB{
		idx: 0,
	}

	mrDB.master = &conn{
		db:  connect(cfg.DN, cfg.MasterDSN),
		dsn: cfg.MasterDSN,
	}

	if cfg.ReadDSNs == nil || len(cfg.ReadDSNs) == 0 {
		cfg.ReadDSNs = make([]string, 1)
		cfg.ReadDSNs = append(cfg.ReadDSNs, cfg.MasterDSN)
	}

	mrDB.reads = make([]*conn, 1)
	for _, dsn := range cfg.ReadDSNs {
		mrDB.reads = append(mrDB.reads, &conn{
			db:  connect(cfg.DN, dsn),
			dsn: dsn,
		})
	}

	return mrDB
}

func connect(dn string, dsn string) *sql.DB {
	db, err := sql.Open(dn, dsn)
	if err != nil {
		panic(err)
	}

	return db
}

func (mrDB *MRDB) masterDB() *sql.DB {
	return mrDB.master.db
}

func (mrDB *MRDB) readDBIndex() int64 {
	if len(mrDB.reads) == 0 {
		panic("no read db")
	}

	idx := atomic.AddInt64(&mrDB.idx, 1)

	return idx
}

//主库
func (mrDB *MRDB) Exec(query string, args ...interface{}) (sql.Result, error) {
	return mrDB.ExecContext(context.Background(), query, args)
}

//主库
func (mrDB *MRDB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return mrDB.masterDB().ExecContext(ctx, query, args)
}

//主库
func (mrDB *MRDB) Prepare(query string) (*sql.Stmt, error) {
	return mrDB.PrepareContext(context.Background(), query)
}

//主库
func (mrDB *MRDB) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return mrDB.masterDB().PrepareContext(ctx, query)
}

//从库，从库都失败走主库
func (mrDB *MRDB) Query(query string, args ...interface{}) (rows *sql.Rows, err error) {
	return mrDB.QueryContext(context.Background(), query, args)
}

//从库
func (mrDB *MRDB) QueryContext(ctx context.Context, query string, args ...interface{}) (rows *sql.Rows, err error) {
	idx := mrDB.readDBIndex()

	for index := range mrDB.reads {
		rows, err = mrDB.reads[(int(idx)+index)%len(mrDB.reads)].db.QueryContext(ctx, query, args)
		if err == nil {
			return
		}
	}
	return mrDB.masterDB().QueryContext(ctx, query, args)
}

//从库
func (mrDB *MRDB) QueryRow(query string, args ...interface{}) (row *sql.Row) {
	return mrDB.QueryRowContext(context.Background(), query, args)
}

//从库
func (mrDB *MRDB) QueryRowContext(ctx context.Context, query string, args ...interface{}) (row *sql.Row) {
	idx := mrDB.readDBIndex()

	for index := range mrDB.reads {
		row = mrDB.reads[(int(idx)+index)%len(mrDB.reads)].db.QueryRowContext(ctx, query, args)
		return
	}
	return mrDB.masterDB().QueryRowContext(ctx, query, args)
}

//主库事务
func (mrDB *MRDB) Begin() (*sql.Tx, error) {
	return mrDB.masterDB().Begin()
}
