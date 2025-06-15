package main

import (
	"context"
	"fmt"
	"html/template"
	"log"
	"net/http"

	"github.com/go-redis/redis/v8"
	"github.com/gorilla/websocket"
)

// global vars for entire server
var (
	ctx         = context.Background() // context manager for redis
	redisClient *redis.Client
	upgrader    = websocket.Upgrader{}
	templates   = template.Must(template.ParseFiles("page.html")) // template renderer
)

func main() {

	redisClient = redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	http.HandleFunc("/", pageHandler)
	http.HandleFunc("/ws/", wsHandler)

	fmt.Println("Server running on port 3000")
	log.Fatal(http.ListenAndServe("127.0.0.1:3000", nil))
}
