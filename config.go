package redisc

import "time"

// Config represent redisc configuration
type Config struct {
	Host               string
	RetryCount         int
	RetryDuration      int
	MaxActive          int
	MaxIdle            int
	IdleTimeout        int
	DialConnectTimeout int
	DialWriteTimeout   int
	DialReadTimeout    int
	DialDatabase       int
	DialKeepAlive      int
	DialPassword       string
	CmdTimeout         time.Duration
}
