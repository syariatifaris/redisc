### Recommendation Config
```
[Redisc "platform"]  
Host = "127.0.0.1:6379" //dummy server  
RetryCount = 100 // please set it to 100, this when MOVED or err operation happenned, we want a maximum 100ms wait, so we do 100 x 1ms  
RetryDuration = 1 // please set it to 100, this when MOVED or err operation happenned, we want a maximum 100ms wait, so we do 100 x 1ms  
MaxActive = 50  
MaxIdle = 50  
IdleTimeout = 240  
DialConnectTimeout = 5  
```
### How to use Redis Sync

Use redsync to avoid cache stampade problem. This is the sample:
```
rcli, err := redisc.New(&redisc.Config{
    Host:               "devel-redis-cluster.tkpd:6379",
    RetryCount:         100,
    RetryDuration:      1,
    MaxActive:          50,
    MaxIdle:            50,
    IdleTimeout:        240,
    DialConnectTimeout: 10,
})
if err != nil{
    log.Fatal(err)
}
data, err := rcli.Get("tx_kudo_222")
if err != nil {
    if err != redis.ErrNil {
        log.Fatal(err)
    }
    if rs := rcli.GetRedSync(); rs != nil {
        mux := rs.NewMutex("sync lock", redsync.SetExpiry(time.Second*5), redsync.SetTries(1))
        if err := mux.Lock(); err != nil {
            log.Fatal(fmt.Sprintf("unable to lock redis, %s", err.Error()))
        }
        log.Println("do something, i.e: get from db")
        mux.Unlock()
    }
}
log.Println(data)
```
