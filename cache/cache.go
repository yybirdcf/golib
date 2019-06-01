package cache

type Cache interface {
	Get(string) ([]byte, error)
	Set(string, []byte, int32) error
	Del(string) error
	Decr(string, uint64) (uint64, error)
	Incr(string, uint64) (uint64, error)
}
