package main

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
)

var (
	ctx         = context.Background()
	redisClient *redis.Client
	countKeys   []string
	keyPrefix   = "leetwatch:viewers:"
	mu          sync.Mutex
)

func updateCountKeys() {
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()

		for range ticker.C {
			startTime := time.Now()
			channels, _ := redisClient.PubSubChannels(ctx, keyPrefix+"*").Result()
			mu.Lock()
			countKeys = channels
			mu.Unlock()
			timeElapsed := time.Since(startTime)
			log.Printf("CountKeys updated. Total keys = %d, Took %s\n", len(countKeys), timeElapsed)
		}
	}()
}

func publishUpdatedCount() {
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for range ticker.C {
			startTime := time.Now()

			mu.Lock()
			keys := make([]string, len(countKeys))
			copy(keys, countKeys)
			mu.Unlock()

			for _, key := range keys {
				val, err := redisClient.Get(ctx, key).Result()
				if err != nil {
					log.Printf("Failed to get value for key %s: %v", key, err)
					continue
				}

				err = redisClient.Publish(ctx, key, val).Err()
				if err != nil {
					log.Printf("Failed to publish to channel %s: %v", key, err)
				}
			}
			elapsedTime := time.Since(startTime)
			log.Printf("Published to %d channels. Took %s\n\n", len(keys), elapsedTime)
		}
	}()
}

func main() {
	redisClient = redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	})

	updateCountKeys()
	publishUpdatedCount()

	select {}
}
