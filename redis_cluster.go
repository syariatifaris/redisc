package redisc

import (
	"time"

	"encoding/json"

	"github.com/go-redsync/redsync"
	redigo "github.com/gomodule/redigo/redis"
	"github.com/mna/redisc"
)

const DefaultCmdTimeout = time.Second * 10

// Cluster represents cluster client
type Cluster struct {
	clusterPool        *redisc.Cluster
	retryCount         int
	retryDuration      time.Duration
	customTimeout      time.Duration
	dialConnectTimeout time.Duration
	RedSync            *redsync.Redsync
}

//New creates redis cluster client
func New(cfg *Config) (Redis, error) {
	pool := redisc.Cluster{
		StartupNodes: []string{cfg.Host},
		DialOptions:  genDialOption(cfg),
		CreatePool: func(address string, opts ...redigo.DialOption) (*redigo.Pool, error) {
			rpool := &redigo.Pool{
				MaxActive:   cfg.MaxActive,
				MaxIdle:     cfg.MaxIdle,
				IdleTimeout: time.Duration(cfg.IdleTimeout) * time.Second,
				Dial: func() (redigo.Conn, error) {
					c, err := redigo.Dial("tcp", address, opts...)
					if err != nil {
						return nil, err
					}
					return c, err
				},
				TestOnBorrow: func(c redigo.Conn, t time.Time) error {
					if time.Since(t) < time.Second {
						return nil
					}
					_, err := c.Do("PING")
					return err
				},
			}
			if _, err := rpool.Dial(); err != nil {
				err = rpool.Close()
				return nil, err
			}
			return rpool, nil
		},
	}
	if err := pool.Refresh(); err != nil {
		return nil, err
	}
	return &Cluster{
		clusterPool:        &pool,
		retryCount:         cfg.RetryCount,
		retryDuration:      time.Duration(cfg.RetryDuration),
		customTimeout:      cfg.CmdTimeout,
		dialConnectTimeout: time.Duration(cfg.DialConnectTimeout),
		RedSync:            redsync.New([]redsync.Pool{&pool}),
	}, nil
}

func genDialOption(redisCfg *Config) []redigo.DialOption {
	options := []redigo.DialOption{}
	if redisCfg.DialConnectTimeout != 0 {
		options = append(options, redigo.DialConnectTimeout(time.Duration(redisCfg.DialConnectTimeout)*time.Second))
	}
	if redisCfg.DialWriteTimeout != 0 {
		options = append(options, redigo.DialWriteTimeout(time.Duration(redisCfg.DialWriteTimeout)*time.Second))
	}
	if redisCfg.DialReadTimeout != 0 {
		options = append(options, redigo.DialReadTimeout(time.Duration(redisCfg.DialReadTimeout)*time.Second))
	}
	if redisCfg.DialDatabase != 0 {
		options = append(options, redigo.DialDatabase(redisCfg.DialDatabase))
	}
	if redisCfg.DialKeepAlive != 0 {
		options = append(options, redigo.DialKeepAlive(time.Duration(redisCfg.DialKeepAlive)*time.Second))
	}
	if redisCfg.DialPassword != "" {
		options = append(options, redigo.DialPassword(redisCfg.DialPassword))
	}
	return options
}

// ClusterStatus get cluster status
func (c *Cluster) ClusterStatus() ([]byte, error) {
	return json.MarshalIndent(c.clusterPool.Stats(), "", "    ")
}
