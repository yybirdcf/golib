package database

import (
	"context"
	"errors"
	"fmt"
)

//根据分区key返回dbname
type ShardingDBFunc func(string) string

//兼容主从 + 自定义字段分库操作db，分表自行业务逻辑里面，或者model里面完成
type DB struct {
	mrDB *MRDB
	ctx  context.Context
	name string
}

func (db *DB) MRDB() *MRDB {
	return db.mrDB
}

type DBManager struct {
	dbs      map[string]*DB
	ctx      context.Context
	sharding map[string]ShardingDBFunc
}

func NewDBManager() *DBManager {
	return &DBManager{
		dbs:      make(map[string]*DB),
		sharding: make(map[string]ShardingDBFunc),
	}
}

//添加db配置, db name + config
func (manger *DBManager) AddDB(dbname string, cfg *MRDBConfig) {
	if _, ok := manger.dbs[dbname]; ok {
		return
	}

	db := &DB{
		ctx:  manger.ctx,
		name: dbname,
		mrDB: NewMRDB(cfg),
	}

	manger.dbs[dbname] = db
}

//表名确定sharding func，key确定sharding到哪个db
func (manger *DBManager) RegisterSharding(tbname string, f ShardingDBFunc) {
	if _, ok := manger.sharding[tbname]; ok {
		return
	}

	manger.sharding[tbname] = f
}

//根据表名获取db
func (manger *DBManager) GetDB(tbname string, key string) (*DB, error) {
	if f, ok := manger.sharding[tbname]; ok {
		dbname := f(key)
		if db, ok := manger.dbs[dbname]; ok {
			return db, nil
		}
		return nil, errors.New(fmt.Sprint("%s can not found", dbname))
	}

	return nil, errors.New(fmt.Sprint("%s can not found", tbname))
}
