package redisc

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/go-redsync/redsync"
	"github.com/gomodule/redigo/redis"
	"github.com/mna/redisc"
)

// Del do DEL command to redis
// Cluster does not support multi command, will invesitage later
func (c *Cluster) Del(key string) error {
	client := c.clusterPool.Get()
	defer client.Close()
	if client.Err() != nil {
		return client.Err()
	}

	rc, err := redisc.RetryConn(client, c.retryCount, c.retryDuration*time.Millisecond)
	if err != nil {
		return err
	}

	resp, err := redis.Int(rc.Do("DEL", key))
	if err != nil {
		return err
	}

	if resp < 0 {
		return fmt.Errorf("unexpected redis response %d", resp)
	}

	return nil
}

func (c *Cluster) Ping() (string, error) {
	client := c.clusterPool.Get()
	defer client.Close()

	if client.Err() != nil {
		return "", client.Err()
	}

	rc, err := redisc.RetryConn(client, c.retryCount, c.retryDuration*time.Millisecond)
	if err != nil {
		return "", err
	}

	resp, err := redis.String(rc.Do("PING"))
	if err != nil && err != redis.ErrNil {
		return "", err
	}

	return resp, nil
}

// Get do GET command to redis
func (c *Cluster) Get(key string) (string, error) {
	client := c.clusterPool.Get()
	defer client.Close()

	if client.Err() != nil {
		return "", client.Err()
	}

	rc, err := redisc.RetryConn(client, c.retryCount, c.retryDuration*time.Millisecond)
	if err != nil {
		return "", err
	}

	resp, err := redis.String(rc.Do("GET", key))
	if err != nil && err != redis.ErrNil {
		return "", err
	}

	return resp, nil
}

// Set do SETEX command to redis
func (c *Cluster) Set(key string, value string, ttl int) error {
	client := c.clusterPool.Get()
	defer client.Close()

	if client.Err() != nil {
		return client.Err()
	}

	rc, err := redisc.RetryConn(client, c.retryCount, c.retryDuration*time.Millisecond)
	if err != nil {
		return err
	}

	// set default TTL
	if ttl == 0 {
		ttl = 3600
	}

	resp, err := redis.String(rc.Do("SET", key, value, "EX", ttl))
	if err != nil {
		return err
	}

	if !strings.EqualFold("OK", resp) {
		return fmt.Errorf("unexpected redis response %s", resp)
	}

	return nil
}

// HGet do HGET command to redis
func (c *Cluster) HGet(key, field string) (string, error) {
	client := c.clusterPool.Get()
	defer client.Close()

	if client.Err() != nil {
		return "", client.Err()
	}

	rc, err := redisc.RetryConn(client, c.retryCount, c.retryDuration*time.Millisecond)
	if err != nil {
		return "", err
	}

	resp, err := redis.String(rc.Do("HGET", key, field))
	if err != nil && err != redis.ErrNil {
		return "", err
	}

	return resp, nil
}

// HSet do HSET command to redis
func (c *Cluster) HSet(key string, field string, value string, ttl int) error {
	client := c.clusterPool.Get()
	defer client.Close()

	if client.Err() != nil {
		return client.Err()
	}

	rc, err := redisc.RetryConn(client, c.retryCount, c.retryDuration*time.Millisecond)
	if err != nil {
		return err
	}

	// do HSET
	resp, err := redis.Int(rc.Do("HSET", key, field, value))
	if err != nil {
		return err
	}

	if resp < 0 || resp > 1 {
		return fmt.Errorf("unexpected redis response %d", resp)
	}

	// set default TTL
	if ttl == 0 {
		ttl = 3600
	}

	// do EXPIRE
	respExpire, err := redis.Int(rc.Do("EXPIRE", key, ttl))
	if err != nil {
		return err
	}

	if respExpire < 0 || respExpire > 1 {
		return fmt.Errorf("unexpected redis response %d", respExpire)
	}

	return nil
}

type result struct {
	key string
	val string
	err error
}

//MGet gets multiple redis value from several keys
//redis cluster does not support mget
func (c *Cluster) MGet(keys []interface{}) (ires []string, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(c.dialConnectTimeout*time.Duration(len(keys)))*time.Second)
	defer cancel()
	fchan := make(chan bool)
	var rm sync.Map
	go func() {
		var wg sync.WaitGroup
		for _, key := range keys {
			wg.Add(1)
			go func(k interface{}) {
				defer wg.Done()
				key := fmt.Sprint(k)
				val, err := c.Get(key)
				rm.Store(key, &result{
					val: val,
					err: err,
				})
			}(key)
		}
		wg.Wait()
		fchan <- true
	}()
	select {
	case <-ctx.Done():
		return nil, errors.New("context timeout get multiple keys")
	case <-fchan:
		//maintain order
		var emsgs []string
		for _, k := range keys {
			var str string
			var rerr error
			if val, ok := rm.Load(k); ok {
				if r, ok := val.(*result); ok {
					str = r.val
					rerr = r.err
				}
			}
			ires = append(ires, str)
			if rerr != nil {
				emsgs = append(emsgs, fmt.Sprintf("key=%s, err=%s.", k, rerr.Error()))
			}
		}
		if len(emsgs) > 0 {
			log.Printf("unable to get some or all keys %+v", emsgs)
			return ires, redis.ErrNil
		}
		return ires, nil
	}
}

// HMGet do HMGET command to redis
func (c *Cluster) HMGet(key string, fields []string) ([]string, error) {
	client := c.clusterPool.Get()
	defer client.Close()

	if client.Err() != nil {
		return []string{}, client.Err()
	}

	rc, err := redisc.RetryConn(client, c.retryCount, c.retryDuration*time.Millisecond)
	if err != nil {
		return []string{}, err
	}

	args := make([]interface{}, len(fields)+1)
	args[0] = key
	for i, v := range fields {
		args[i+1] = v
	}

	resp, err := redis.Strings(rc.Do("HMGET", args...))
	if err != nil && err != redis.ErrNil {
		return []string{}, err
	}

	return resp, nil
}

// HMSet do HMSET command to redis
func (c *Cluster) HMSet(key string, ttl int, m map[string]string) error {
	client := c.clusterPool.Get()
	defer client.Close()

	if client.Err() != nil {
		return client.Err()
	}

	rc, err := redisc.RetryConn(client, c.retryCount, c.retryDuration*time.Millisecond)
	if err != nil {
		return err
	}

	// do HMSET
	resp, err := redis.String(rc.Do("HMSET", redis.Args{}.Add(key).AddFlat(m)...))
	if err != nil {
		return err
	}

	if !strings.EqualFold("OK", resp) {
		return fmt.Errorf("unexpected redis response %s", resp)
	}

	// set default TTL
	if ttl == 0 {
		ttl = 3600
	}

	// do EXPIRE
	respExpire, err := redis.Int(rc.Do("EXPIRE", key, ttl))
	if err != nil {
		return err
	}

	if respExpire < 0 || respExpire > 1 {
		return fmt.Errorf("unexpected redis response %d", respExpire)
	}

	return nil
}

// IncrBy do INCRBY command
func (c *Cluster) IncrBy(key string, amount int) (int, error) {
	client := c.clusterPool.Get()
	defer client.Close()

	if client.Err() != nil {
		return 0, client.Err()
	}

	rc, err := redisc.RetryConn(client, c.retryCount, c.retryDuration*time.Millisecond)
	if err != nil {
		return 0, err
	}

	// do INCRBY
	resp, err := redis.Int(rc.Do("INCRBY", key, amount))
	if err != nil {
		return 0, err
	}

	return resp, nil
}

// DecrBy do DECRBY command
func (c *Cluster) DecrBy(key string, amount int) (int, error) {
	client := c.clusterPool.Get()
	defer client.Close()

	if client.Err() != nil {
		return 0, client.Err()
	}

	rc, err := redisc.RetryConn(client, c.retryCount, c.retryDuration*time.Millisecond)
	if err != nil {
		return 0, err
	}

	// do DECRBY
	resp, err := redis.Int(rc.Do("DECRBY", key, amount))
	if err != nil {
		return 0, err
	}

	return resp, nil
}

func (c *Cluster) GetByte(key string) ([]byte, error) {
	return c.GetBytes(key, nil)
}

//GetBytes gets redis data in bytes format
func (c *Cluster) GetBytes(key string, fields []string) ([]byte, error) {
	client := c.clusterPool.Get()
	defer client.Close()

	if client.Err() != nil {
		return nil, client.Err()
	}

	rc, err := redisc.RetryConn(client, c.retryCount, c.retryDuration*time.Millisecond)
	if err != nil {
		return nil, err
	}
	args := make([]interface{}, len(fields)+1)
	args[0] = key
	for i, v := range fields {
		args[i+1] = v
	}
	resp, err := redis.Bytes(rc.Do("GET", args...))
	if err != nil && err != redis.ErrNil {
		return nil, err
	}

	return resp, nil
}

//SetByte sets bytes value with time expiration
func (c *Cluster) SetByte(key string, value []byte, ttl int) error {
	client := c.clusterPool.Get()
	defer client.Close()

	if client.Err() != nil {
		return client.Err()
	}

	rc, err := redisc.RetryConn(client, c.retryCount, c.retryDuration*time.Millisecond)
	if err != nil {
		return err
	}
	resp, err := redis.String(rc.Do("SET", key, value, "EX", ttl))
	if err != nil {
		return err
	}
	if !strings.EqualFold("OK", string(resp)) {
		return fmt.Errorf("unexpected redis response %s", string(resp))
	}
	return nil
}

func (c *Cluster) RPush(key string, args []interface{}) error {
	client := c.clusterPool.Get()
	defer client.Close()

	if client.Err() != nil {
		return client.Err()
	}

	rc, err := redisc.RetryConn(client, c.retryCount, c.retryDuration*time.Millisecond)
	if err != nil {
		return err
	}

	_, err = redis.Int64(rc.Do("RPUSH", redis.Args{key}.AddFlat(args)...))
	if err != nil && err != redis.ErrNil {
		return err
	}

	return nil
}

//RPushEx will do rpush and give expire
func (c *Cluster) RPushEx(key string, args []interface{}, ttl int) error {
	client := c.clusterPool.Get()
	defer client.Close()

	if client.Err() != nil {
		return client.Err()
	}

	rc, err := redisc.RetryConn(client, c.retryCount, c.retryDuration*time.Millisecond)
	if err != nil {
		return err
	}

	_, err = redis.Int64(rc.Do("RPUSH", redis.Args{key}.AddFlat(args)...))
	if err != nil && err != redis.ErrNil {
		return err
	}

	// do EXPIRE
	respExpire, err := redis.Int(rc.Do("EXPIRE", key, ttl))
	if err != nil {
		return err
	}

	if respExpire < 0 || respExpire > 1 {
		return fmt.Errorf("unexpected redis response %d", respExpire)
	}

	return nil
}

func (c *Cluster) LRangeByte(key string, start, stop int64) ([][]byte, error) {
	client := c.clusterPool.Get()
	defer client.Close()

	if client.Err() != nil {
		return [][]byte{}, client.Err()
	}

	rc, err := redisc.RetryConn(client, c.retryCount, c.retryDuration*time.Millisecond)
	if err != nil {
		return [][]byte{}, err
	}

	val, err := redis.ByteSlices(rc.Do("LRANGE", key, start, stop))
	if err != nil && err != redis.ErrNil {
		return [][]byte{}, err
	}

	return val, nil
}

// GetRedSync
func (c *Cluster) GetRedSync() *redsync.Redsync {
	return c.RedSync
}
