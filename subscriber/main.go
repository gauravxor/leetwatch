package main

import (
	"context"
	"html/template"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/gorilla/websocket"
	"github.com/joho/godotenv"
)

var (
	ctx            = context.Background()
	redisClient    *redis.Client
	upgrader             = websocket.Upgrader{}
	templates            = template.Must(template.ParseFiles("subscriber/page.html"))
	redisHealthy   int32 = -1 // 0 -> unhealthy | 1 -> healthy | -1 -> unknown
	activeClients  sync.Map
	broadcasters   = make(map[string]*PageBroadcaster)
	broadcastersMu sync.Mutex
	channelPrefix  = "leetwatch:viewers:"
	countKeyPrefix = "leetwatch:viewers:"
)

func main() {

	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, using defaults")
	}
	redisHost := os.Getenv("REDIS_HOST")
	if redisHost == "" {
		redisHost = "locahost"
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
		Addr: redisHost + ":" + redisPort,
		Password: redisPassword,
		DB:   0,
	})

	err := waitForInitialRedisCheck(5 * time.Second)
	if err != nil {
		log.Fatal("Redis not healthy at startup: ", err)
	}
	startRedisHealthMonitor()

	http.HandleFunc("/", pageHandler)
	http.HandleFunc("/ws/", wsHandler)

	startMetricsLogger()

	port := os.Getenv("SUBSCRIBER_PORT")
	if port == "" {
		port = "8080"
	}
	log.Println("Server running on port " + port)

	log.Fatal(http.ListenAndServe("127.0.0.1:"+port, nil))

}
