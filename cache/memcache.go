package cache

import (
	"errors"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/yybirdcf/golib/utils"
)

type MemCache struct {
	servers []string
	mcs     map[string]*memcache.Client
	nodes   *utils.HashRing
}

func NewMemCache(servers []string) *MemCache {
	m := &MemCache{
		servers: servers,
		mcs:     make(map[string]*memcache.Client),
	}

	m.genRing(servers, 200)

	return m
}

func (m *MemCache) genRing(servers []string, num int) {
	nodes := utils.NewHashRing(num)

	nodesMap := make(map[string]int)
	for _, server := range servers {
		nodesMap[server] = 1
		m.mcs[server] = memcache.New(server)
	}

	nodes.AddNodes(nodesMap)
	m.nodes = nodes
}

func (m *MemCache) node(key string) (*memcache.Client, error) {
	if c, ok := m.mcs[m.nodes.GetNode(key)]; ok {
		return c, nil
	}
	return nil, errors.New("memcache node not found")
}

func (m *MemCache) Get(key string) ([]byte, error) {
	node, err := m.node(key)
	if err != nil {
		return nil, err
	}

	item, err := node.Get(key)
	if err != nil {
		return nil, err
	}

	return item.Value, nil
}

//过期时间秒数，0表示不过期
func (m *MemCache) Set(key string, value []byte, expiration int32) error {
	node, err := m.node(key)
	if err != nil {
		return err
	}

	item := &memcache.Item{
		Key:        key,
		Value:      value,
		Expiration: expiration,
	}

	return node.Set(item)
}

func (m *MemCache) Del(key string) error {
	node, err := m.node(key)
	if err != nil {
		return err
	}

	return node.Delete(key)
}

func (m *MemCache) Decr(key string, delta uint64) (uint64, error) {
	node, err := m.node(key)
	if err != nil {
		return 0, err
	}

	return node.Decrement(key, delta)
}

func (m *MemCache) Incr(key string, delta uint64) (uint64, error) {
	node, err := m.node(key)
	if err != nil {
		return 0, err
	}

	return node.Increment(key, delta)
}
