package cache

import (
	"errors"

	"github.com/yybirdcf/golib/utils"
)

type FileCache struct {
	rootPaths []string
	mcs       map[string]*utils.FileCacher
	nodes     *utils.HashRing
}

func NewFileCache(rootPaths []string) *FileCache {
	m := &FileCache{
		rootPaths: rootPaths,
		mcs:       make(map[string]*utils.FileCacher),
	}

	m.genRing(rootPaths, 200)

	return m
}

func (m *FileCache) genRing(rootPaths []string, num int) {
	nodes := utils.NewHashRing(num)

	nodesMap := make(map[string]int)
	for _, path := range rootPaths {
		nodesMap[path] = 1
		m.mcs[path] = utils.NewFileCacher(path)
	}

	nodes.AddNodes(nodesMap)
	m.nodes = nodes
}

func (m *FileCache) node(key string) (*utils.FileCacher, error) {
	if c, ok := m.mcs[m.nodes.GetNode(key)]; ok {
		return c, nil
	}
	return nil, errors.New("file node not found")
}

func (m *FileCache) Get(key string) ([]byte, error) {
	node, err := m.node(key)
	if err != nil {
		return nil, err
	}

	bytes, ok := node.Get(key).([]byte)
	if ok {
		return bytes, nil
	}

	return nil, errors.New("file get bytes error")
}

//过期时间秒数，0表示不过期
func (m *FileCache) Set(key string, value []byte, expiration int32) error {
	node, err := m.node(key)
	if err != nil {
		return err
	}

	return node.Put(key, value, int64(expiration))
}

func (m *FileCache) Del(key string) error {
	node, err := m.node(key)
	if err != nil {
		return err
	}

	return node.Delete(key)
}

func (m *FileCache) Decr(key string, delta uint64) (uint64, error) {
	node, err := m.node(key)
	if err != nil {
		return 0, err
	}

	return node.Decr(key, delta)
}

func (m *FileCache) Incr(key string, delta uint64) (uint64, error) {
	node, err := m.node(key)
	if err != nil {
		return 0, err
	}

	return node.Incr(key, delta)
}
