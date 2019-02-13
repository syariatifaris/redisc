package redisc

import (
	"github.com/go-redsync/redsync"
)

//Redis as redis abstraction contract
//we can use this abstraction if when adding new client
type Redis interface {
	Ping() (string, error)
	Get(key string) (string, error)
	Del(key string) error
	Set(key, value string, expire int) error
	MGet(keys []interface{}) ([]string, error)
	SetByte(key string, value []byte, expire int) error
	GetByte(key string) ([]byte, error)
	RPush(key string, args []interface{}) error
	RPushEx(key string, args []interface{}, ttl int) error
	LRangeByte(key string, start, stop int64) ([][]byte, error)
	GetRedSync() *redsync.Redsync
}
