package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

type ShardingFunc func(string) *DB

type DB struct {
	*sql.DB
	ctx  context.Context
	dsn  string
	name string
}

func (d *DB) connect() {
	db, err := sql.Open("mysql", d.dsn)
	if err != nil {
		panic(err)
		return
	}

	d.DB = db
}

type DBManager struct {
	dbs      map[string]*DB
	ctx      context.Context
	sharding map[string]ShardingFunc
}

func NewDBManager() *DBManager {
	return &DBManager{
		dbs:      make(map[string]*DB),
		sharding: make(map[string]ShardingFunc),
	}
}

//添加db配置
func (manger *DBManager) InitDB(name string, dsn string) {
	if _, ok := manger.dbs[name]; ok {
		return
	}

	db := &DB{
		ctx:  manger.ctx,
		dsn:  dsn,
		name: name,
	}
	db.connect()

	manger.dbs[name] = db
}

//表名确定sharding func，key确定sharding到哪个db
func (manger *DBManager) RegisterSharding(name string, f ShardingFunc) {
	if _, ok := manger.sharding[name]; ok {
		return
	}

	manger.sharding[name] = f
}

//获取db
func (manger *DBManager) GetDB(name string, key string) (*DB, error) {
	if f, ok := manger.sharding[name]; ok {
		return f(key), nil
	}

	return nil, errors.New(fmt.Sprint("%s can not found", name))
}
