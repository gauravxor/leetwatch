package main

import (
	"context"
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
	ctx            = context.Background() // context manager for redis
	redisClient    *redis.Client
	upgrader             = websocket.Upgrader{}
	templates            = template.Must(template.ParseFiles("subscriber/page.html"))
	redisHealthy   int32 = -1 // 0 -> unhealthy | 1 -> healthy | -1 -> unknown
	activeClients  sync.Map
	broadcasters   = make(map[string]*PageBroadcaster)
	broadcastersMu sync.Mutex
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

	http.HandleFunc("/", pageHandler)
	http.HandleFunc("/ws/", wsHandler)

	log.Println("Server running on port 3000")

	go startMetricsLogger("withBroadcast")

	log.Fatal(http.ListenAndServe("127.0.0.1:3000", nil))

}
