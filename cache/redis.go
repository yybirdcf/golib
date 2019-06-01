package cache

import (
	"errors"
	"fmt"
	"time"

	"../utils"
	"github.com/gomodule/redigo/redis"
)

type RedisConfig struct {
	host     string
	port     int
	password string
	db       int
}

type RedisCache struct {
	servers []RedisConfig
	rcs     map[string]*redis.Pool
	nodes   *utils.HashRing
}

func NewRedisCache(servers []RedisConfig) *RedisCache {
	r := &RedisCache{
		servers: servers,
		rcs:     make(map[string]*redis.Pool),
	}

	r.genRing(servers)

	return r
}

func (r *RedisCache) genRing(servers []RedisConfig) {
	nodes := utils.NewHashRing(200)

	nodesMap := make(map[string]int)
	for _, server := range servers {
		srv := fmt.Sprintf("%s:%d", server.host, server.port)
		nodesMap[srv] = 1
		r.rcs[srv] = &redis.Pool{
			MaxIdle:     25,
			MaxActive:   500,
			IdleTimeout: time.Duration(time.Second * 360),
			Dial: func() (redis.Conn, error) {
				c, err := redis.Dial("tcp", srv)
				if err != nil {
					return nil, err
				}
				if server.password != "" {
					if _, err := c.Do("AUTH", server.password); err != nil {
						c.Close()
						return nil, err
					}
				}

				if _, err := c.Do("SELECT", server.db); err != nil {
					c.Close()
					return nil, err
				}
				return c, nil
			},
		}
	}

	nodes.AddNodes(nodesMap)
	r.nodes = nodes
}

func (r *RedisCache) node(key string) (*redis.Pool, error) {
	if c, ok := r.rcs[r.nodes.GetNode(key)]; ok {
		return c, nil
	}
	return nil, errors.New("redis node not found")
}

func (r *RedisCache) Get(key string) ([]byte, error) {
	node, err := r.node(key)
	if err != nil {
		return nil, err
	}

	conn := node.Get()
	defer conn.Close()

	return redis.Bytes(conn.Do("GET", key))
}

//过期时间秒数，0表示不过期
func (r *RedisCache) Set(key string, value []byte, expiration int32) error {
	node, err := r.node(key)
	if err != nil {
		return err
	}

	conn := node.Get()
	defer conn.Close()

	if expiration == 0 {
		_, err = conn.Do("SET", key, value)
	} else {
		_, err = conn.Do("SET", key, value, "EX", expiration)
	}
	return err
}

func (r *RedisCache) Del(key string) error {
	node, err := r.node(key)
	if err != nil {
		return err
	}

	conn := node.Get()
	defer conn.Close()

	_, err = conn.Do("DEL", key)
	return err
}

func (r *RedisCache) Decr(key string, delta uint64) (uint64, error) {
	node, err := r.node(key)
	if err != nil {
		return 0, err
	}

	conn := node.Get()
	defer conn.Close()

	return redis.Uint64(conn.Do("DECRBY", key, delta))
}

func (r *RedisCache) Incr(key string, delta uint64) (uint64, error) {
	node, err := r.node(key)
	if err != nil {
		return 0, err
	}

	conn := node.Get()
	defer conn.Close()

	return redis.Uint64(conn.Do("INCRBY", key, delta))
}
