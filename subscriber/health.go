package main

import (
	"fmt"
	"log"
	"sync/atomic"
	"time"
)

func startRedisHealthMonitor() {
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()

		for range ticker.C {
			err := redisClient.Ping(ctx).Err()
			if err != nil {
				log.Println("Redis unhealty", err)
				if atomic.SwapInt32(&redisHealthy, 0) != 0 {
					log.Println("Redis became unhealthy. Closing all clients.")
					closeAllActiveClients()
				}
			} else {
				atomic.StoreInt32(&redisHealthy, 1)
			}
		}
	}()
}

func waitForInitialRedisCheck(timeout time.Duration) error {
	log.Println("Waiting for redis health check to succeed")
	start := time.Now()
	for {
		err := redisClient.Ping(ctx).Err()
		if err == nil {
			log.Println("Redis health check succeeded")
			atomic.StoreInt32(&redisHealthy, 1)
			return nil
		}
		if time.Since(start) > timeout {
			return fmt.Errorf("timeout waiting for Redis")
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func isRedisHealthy() bool {
	return atomic.LoadInt32(&redisHealthy) == 1
}
