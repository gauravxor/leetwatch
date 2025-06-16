package main

import (
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"

	"github.com/go-redis/redis/v8"
	"github.com/gorilla/websocket"
)

type wsClient struct {
	conn      *websocket.Conn // the actual ws connection
	pageName  string          // the page being viewed through the current ws client instance
	sub       *redis.PubSub   // redis pubsub subscription instance
	closeOnce sync.Once       // cleanup
}

func wsHandler(w http.ResponseWriter, r *http.Request) {
	// check if redis is working properly, otherwise close the connection
	if !isRedisHealthy() {
		http.Error(w, "Service unavailable", http.StatusServiceUnavailable)
		log.Println("Redis unhealthy")
		return
	}

	// allow ws connection if the redis server is working fine
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("Error upgrading ws connection: ", err)
		return
	}
	log.Println("New WS client connected.")

	pageName := strings.TrimPrefix(r.URL.Path, "/ws/")
	channel := "leetwatch:viewers:" + pageName
	countKey := channel

	// Increment view count
	updatedCount, err := redisClient.Incr(ctx, countKey).Result()
	fmt.Println("The new updated count = ", updatedCount)
	if err != nil {
		log.Println("Redis INCR error:", err)
		conn.Close() // closing WS connection in case of error
		return
	}

	// publish count in the channel for current page
	redisClient.Publish(ctx, channel, fmt.Sprintf("%d", updatedCount))

	// send the count to the current connection
	err = conn.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf("%d", updatedCount)))

	// subscribe to the channel for future updates
	sub := redisClient.Subscribe(ctx, channel)
	ch := sub.Channel()

	client := &wsClient{
		conn:     conn,
		pageName: pageName,
		sub:      sub,
	}
	activeClients.Store(client, struct{}{})

	// listen for published messages in thd channel
	go func() {
		for msg := range ch {
			err := conn.WriteMessage(websocket.TextMessage, []byte(msg.Payload))
			if err != nil {
				return
			}
		}
	}()

	// handle closing of ws connection
	go func() {
		defer client.close()
		for {
			_, _, err := conn.ReadMessage()
			if err != nil {
				return
			}
		}
	}()
}

func (c *wsClient) close() {
	fmt.Println("Closing/cleaning up WS connection")

	c.closeOnce.Do(func() {
		activeClients.Delete(c)

		countKey := "leetwatch:viewers:" + c.pageName
		channel := countKey

		// decrement the view counter
		updatedCount, err := redisClient.Decr(ctx, countKey).Result()
		if err == nil {
			fmt.Println("Disconnecting -> New count = ", updatedCount)
			// publish the decreased value in the channel
			redisClient.Publish(ctx, channel, fmt.Sprintf("%d", updatedCount))
		}
		c.sub.Close()
		c.conn.Close()
		log.Println("WebSocket closed for", c.pageName)
	})
}

func closeAllActiveClients() {
	activeClients.Range(func(key, _ any) bool {
		client := key.(*wsClient)
		go client.close()
		return true
	})
}
