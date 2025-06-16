package main

import (
	"context"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/gorilla/websocket"
)

// global vars for entire server
var (
	ctx          = context.Background() // context manager for redis
	redisClient  *redis.Client
	upgrader           = websocket.Upgrader{}
	templates          = template.Must(template.ParseFiles("page.html"))
	redisHealthy int32 = -1 // 0 -> unhealthy | 1 -> healthy | -1 -> unknown
	activeClients sync.Map
)

func main() {

	redisClient = redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	})

	err := waitForInitialRedisCheck(5 * time.Second)
	if err != nil {
		log.Fatal("Redis not healthy at startup: ", err)
	}
	startRedisHealthMonitor()

	/**
	Flushing redis on server reboot is bad design in a micrservice
	architecture, where multiple servers are added and removed as per load.
	During each addition, the server would ideally reboot, leading to a critical issue

	Keys should be flushed by redis itself, in case of failures.
	*/

	err = redisClient.FlushAll(ctx).Err()
	if err != nil {
		log.Fatalf("Failed to flush Redis: %v", err)
	}

	http.HandleFunc("/", pageHandler)
	http.HandleFunc("/ws/", wsHandler)

	fmt.Println("Server running on port 3000")

	log.Fatal(http.ListenAndServe("127.0.0.1:3000", nil))
}
