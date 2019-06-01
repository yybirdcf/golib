package utils

import (
	"bytes"
	"crypto/md5"
	"encoding/gob"
	"encoding/hex"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/Unknwon/com"
)

// Item represents a cache item.
type Item struct {
	Val     interface{}
	Created int64
	Expire  int64
}

func (item *Item) hasExpired() bool {
	return item.Expire > 0 &&
		(time.Now().Unix()-item.Created) >= item.Expire
}

// FileCacher represents a file cache adapter implementation.
type FileCacher struct {
	lock     sync.Mutex
	rootPath string
	interval int // GC interval.
}

// NewFileCacher creates and returns a new file cacher.
func NewFileCacher(rootPath string) *FileCacher {

	cache := &FileCacher{}
	cache.StartAndGC(rootPath, 180)

	return cache
}

func (c *FileCacher) filepath(key string) string {
	m := md5.Sum([]byte(key))
	hash := hex.EncodeToString(m[:])
	return filepath.Join(c.rootPath, string(hash[0]), string(hash[1]), hash)
}

// Put puts value into cache with key and expire time.
// If expired is 0, it will be deleted by next GC operation.
func (c *FileCacher) Put(key string, val interface{}, expire int64) error {
	filename := c.filepath(key)
	item := &Item{val, time.Now().Unix(), expire}
	data, err := EncodeGob(item)
	if err != nil {
		return err
	}

	os.MkdirAll(filepath.Dir(filename), os.ModePerm)
	return ioutil.WriteFile(filename, data, os.ModePerm)
}

func (c *FileCacher) read(key string) (*Item, error) {
	filename := c.filepath(key)

	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	item := new(Item)
	return item, DecodeGob(data, item)
}

// Get gets cached value by given key.
func (c *FileCacher) Get(key string) interface{} {
	item, err := c.read(key)
	if err != nil {
		return nil
	}

	if item.hasExpired() {
		os.Remove(c.filepath(key))
		return nil
	}
	return item.Val
}

// Delete deletes cached value by given key.
func (c *FileCacher) Delete(key string) error {
	return os.Remove(c.filepath(key))
}

// Incr increases cached int-type value by given key as a counter.
func (c *FileCacher) Incr(key string, delta uint64) (uint64, error) {
	item, err := c.read(key)
	if err != nil {
		return 0, err
	}

	item.Val, err = Incr(item.Val, delta)
	if err != nil {
		return 0, err
	}

	val, ok := item.Val.(uint64)
	if !ok {
		return 0, errors.New("FileCacher Decr type uint64 failed")
	}

	return val, c.Put(key, item.Val, item.Expire)
}

// Decrease cached int value.
func (c *FileCacher) Decr(key string, delta uint64) (uint64, error) {
	item, err := c.read(key)
	if err != nil {
		return 0, err
	}

	item.Val, err = Decr(item.Val, delta)
	if err != nil {
		return 0, err
	}

	val, ok := item.Val.(uint64)
	if !ok {
		return 0, errors.New("FileCacher Decr type uint64 failed")
	}

	return val, c.Put(key, item.Val, item.Expire)
}

// IsExist returns true if cached value exists.
func (c *FileCacher) IsExist(key string) bool {
	return com.IsExist(c.filepath(key))
}

// Flush deletes all cached data.
func (c *FileCacher) Flush() error {
	return os.RemoveAll(c.rootPath)
}

func (c *FileCacher) startGC() {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.interval < 1 {
		return
	}

	if err := filepath.Walk(c.rootPath, func(path string, fi os.FileInfo, err error) error {
		if err != nil {
			return fmt.Errorf("Walk: %v", err)
		}

		if fi.IsDir() {
			return nil
		}

		data, err := ioutil.ReadFile(path)
		if err != nil && !os.IsNotExist(err) {
			fmt.Errorf("ReadFile: %v", err)
		}

		item := new(Item)
		if err = DecodeGob(data, item); err != nil {
			return err
		}
		if item.hasExpired() {
			if err = os.Remove(path); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("Remove: %v", err)
			}
		}
		return nil
	}); err != nil {
		log.Printf("error garbage collecting cache files: %v", err)
	}

	time.AfterFunc(time.Duration(c.interval)*time.Second, func() { c.startGC() })
}

// StartAndGC starts GC routine based on config string settings.
func (c *FileCacher) StartAndGC(rootPath string, interval int) error {
	c.lock.Lock()
	c.rootPath = rootPath
	c.interval = interval

	if !filepath.IsAbs(c.rootPath) {
		c.rootPath = filepath.Join("/", c.rootPath)
	}
	c.lock.Unlock()

	if err := os.MkdirAll(c.rootPath, os.ModePerm); err != nil {
		return err
	}

	go c.startGC()
	return nil
}

func EncodeGob(item *Item) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	err := gob.NewEncoder(buf).Encode(item)
	return buf.Bytes(), err
}

func DecodeGob(data []byte, out *Item) error {
	buf := bytes.NewBuffer(data)
	return gob.NewDecoder(buf).Decode(&out)
}

func Incr(val interface{}, delta uint64) (interface{}, error) {
	switch val.(type) {
	case int:
		val = val.(int) + int(delta)
	case int32:
		val = val.(int32) + int32(delta)
	case int64:
		val = val.(int64) + int64(delta)
	case uint:
		val = val.(uint) + uint(delta)
	case uint32:
		val = val.(uint32) + uint32(delta)
	case uint64:
		val = val.(uint64) + uint64(delta)
	default:
		return val, errors.New("item value is not int-type")
	}
	return val, nil
}

func Decr(val interface{}, delta uint64) (interface{}, error) {
	switch val.(type) {
	case int:
		val = val.(int) - int(delta)
	case int32:
		val = val.(int32) - int32(delta)
	case int64:
		val = val.(int64) - int64(delta)
	case uint:
		if val.(uint) > 0 {
			val = val.(uint) - uint(delta)
		} else {
			return val, errors.New("item value is less than 0")
		}
	case uint32:
		if val.(uint32) > 0 {
			val = val.(uint32) - uint32(delta)
		} else {
			return val, errors.New("item value is less than 0")
		}
	case uint64:
		if val.(uint64) > 0 {
			val = val.(uint64) - uint64(delta)
		} else {
			return val, errors.New("item value is less than 0")
		}
	default:
		return val, errors.New("item value is not int-type")
	}
	return val, nil
}
