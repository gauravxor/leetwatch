package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/joho/godotenv"
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

func healthHandler(w http.ResponseWriter, r *http.Request) {
	status := "OK"
	redisStatus := "OK"

	_, err := redisClient.Ping(ctx).Result()
	if err != nil {
		status = "Unhealthy"
		redisStatus = "Redis connection failed: " + err.Error()
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"` + status + `","redis":"` + redisStatus + `"}`))
}

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, using defaults")
	}
	redisHost := os.Getenv("REDIS_HOST")
	if redisHost == "" {
		redisHost = "localhost"
	}
	redisPort := os.Getenv("REDIS_PORT")
	if redisPort == "" {
		redisPort = "6379"
	}
	redisPassword := os.Getenv("REDIS_PASSWORD")
	if redisPassword == "" {
		redisPassword = ""
	}
	redisClient = redis.NewClient(&redis.Options{
		Addr:     redisHost + ":" + redisPort,
		Password: redisPassword,
		DB:       0,
	})

	updateCountKeys()
	publishUpdatedCount()

	http.HandleFunc("/health", healthHandler)

	host := os.Getenv("PUBLISHER_HOST")
	if host == "" {
		host = "0.0.0.0"
	}
	port := os.Getenv("PUBLISHER_PORT")
	if port == "" {
		port = "8080"
	}
	log.Println("Server starting on port " + port)
	if err := http.ListenAndServe(host+":"+port, nil); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
