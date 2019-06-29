package queue

import (
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/yybirdcf/golib/clog"
)

type RedisConfig struct {
	Host     string
	Password string
	Db       int
}

type RedisQueue struct {
	pool     *redis.Pool
	handlers map[string]func(interface{})
}

func NewRedisQueue(cfg *RedisConfig) *RedisQueue {
	pool := &redis.Pool{
		MaxIdle:     25,
		MaxActive:   500,
		IdleTimeout: time.Duration(time.Second * 360),
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", cfg.Host)
			if err != nil {
				return nil, err
			}
			if cfg.Password != "" {
				if _, err := c.Do("AUTH", cfg.Password); err != nil {
					c.Close()
					return nil, err
				}
			}

			if _, err := c.Do("SELECT", cfg.Db); err != nil {
				c.Close()
				return nil, err
			}
			return c, nil
		},
	}

	rq := &RedisQueue{}
	rq.pool = pool
	rq.handlers = make(map[string]func(interface{}))

	return rq
}

func (rq *RedisQueue) Push(name string, value string) error {
	conn := rq.pool.Get()
	defer conn.Close()

	_, err := conn.Do("RPUSH", name, value)
	return err
}

func (rq *RedisQueue) RegisterHandler(name string, handler func(interface{})) {
	rq.handlers[name] = handler
}

func (rq *RedisQueue) Run() {
	for name, _ := range rq.handlers {
		rq.runHandler(name)
	}
}

func (rq *RedisQueue) runHandler(name string) {
	go func(name string) {
		clog.Info(name)
		conn := rq.pool.Get()
		defer conn.Close()
		for {
			res, err := redis.String(conn.Do("LPOP", name))
			if err != nil {
				if err == redis.ErrNil {
					time.Sleep(time.Second * 1)
				} else {
					clog.Error(err)
				}
				continue
			}

			rq.handlers[name](res)
		}
	}(name)
}

func (rq *RedisQueue) Close() {
	if rq.pool != nil {
		rq.pool.Close()
	}
}
