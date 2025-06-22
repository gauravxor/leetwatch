package main

import (
	"context"
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

// to store the channel subscription connection for a page.
// this connection would be reused across all ws clients listening
// for a page.
type PageBroadcaster struct {
	pageName string
	clients  map[*wsClient]struct{} // the ws clients
	mu       sync.Mutex             // for groutine safety
	sub      *redis.PubSub          // the channel subscription instance
	ctx      context.Context
	cancel   context.CancelFunc
}

func wsHandler(resp http.ResponseWriter, req *http.Request) {
	// is redis is not healthy, reject the connection request
	if !isRedisHealthy() {
		log.Println("Redis unhealthy")
		http.Error(resp, "Service unavailable", http.StatusServiceUnavailable)
		return
	}

	// allow the connection
	conn, err := upgrader.Upgrade(resp, req, nil)
	if err != nil {
		log.Println("Error upgrading ws connection: ", err)
		return
	}

	log.Println("Client connected.")

	pageName := strings.TrimPrefix(req.URL.Path, "/ws/")
	channel := "leetwatch:viewers:" + pageName
	countKey := channel

	// increase the count of viewers/subscribers for the given page
	updatedCount, err := redisClient.Incr(ctx, countKey).Result()
	log.Println("The new updated count = ", updatedCount)
	if err != nil {
		log.Println("Redis INCR error:", err)
		conn.Close() // closing WS connection in case of error
		return
	}

	// publish count in the channel for current page
	redisClient.Publish(ctx, channel, fmt.Sprintf("%d", updatedCount))

	// send the count to the current connection
	conn.WriteMessage(websocket.TextMessage, fmt.Appendf(nil, "%d", updatedCount))

	client := &wsClient{
		conn:     conn,
		pageName: pageName,
	}

	activeClients.Store(client, struct{}{})

	// get the broadcaster for the current page
	broadcaster := getOrCreateBroadcaster(pageName)
	// add the WS client object to broadcaster
	broadcaster.addClient(client)

	go monitorClientDisconnection(client, broadcaster)
}

func (client *wsClient) close() {
	log.Println("Closing/cleaning up WS connection")

	client.closeOnce.Do(func() {
		activeClients.Delete(client)

		countKey := "leetwatch:viewers:" + client.pageName
		channel := countKey

		// decrement the view counter
		updatedCount, err := redisClient.Decr(ctx, countKey).Result()
		if err == nil {
			log.Println("Disconnecting -> New count = ", updatedCount)
			// publish the decreased value in the channel
			redisClient.Publish(ctx, channel, fmt.Sprintf("%d", updatedCount))
		}
		if client.sub != nil {
			client.sub.Close()
		}
		client.conn.Close()
		log.Println("WebSocket closed for", client.pageName)
	})
}

func closeAllActiveClients() {
	activeClients.Range(func(key, _ any) bool {
		client := key.(*wsClient)
		go client.close()
		return true
	})
}

func monitorClientDisconnection(client *wsClient, broadcaster *PageBroadcaster) {
	for {
		if _, _, err := client.conn.ReadMessage(); err != nil {
			client.conn.Close()
			broadcaster.removeClient(client)
			return
		}
	}
}

func getOrCreateBroadcaster(pageName string) *PageBroadcaster {
	broadcastersMu.Lock()
	defer broadcastersMu.Unlock()

	// check if any existing broadcaster is there for the requested page
	if existing, ok := broadcasters[pageName]; ok {
		return existing
	}

	// if not, then create one
	ctx, cancel := context.WithCancel(context.Background())
	channel := "leetwatch:viewers:" + pageName
	redisSub := redisClient.Subscribe(ctx, channel)

	broadcaster := &PageBroadcaster{
		pageName: pageName,
		clients:  make(map[*wsClient]struct{}),
		sub:      redisSub,
		ctx:      ctx,
		cancel:   cancel,
	}

	broadcasters[pageName] = broadcaster
	go broadcaster.listenAndFanOut()
	return broadcaster
}

// for each published message, C no. of clients are looped to
// send the message, leading to (N * C) operations, where N
// is the no. of published messages.
// TODO: Figure out a better way to broadcast the messages
func (broadcaster *PageBroadcaster) listenAndFanOut() {
	// get the redis subscription channel
	messageChan := broadcaster.sub.Channel()

	for msg := range messageChan {
		// fmt.Println("New message came, iterating")
		broadcaster.mu.Lock()
		for client := range broadcaster.clients {
			// log.Println("WRITING")
			client.conn.WriteMessage(websocket.TextMessage, []byte(msg.Payload))
		}
		broadcaster.mu.Unlock()
	}
}

func (broadcaster *PageBroadcaster) addClient(client *wsClient) {
	broadcaster.mu.Lock()
	defer broadcaster.mu.Unlock()
	broadcaster.clients[client] = struct{}{}
}

func (broadcaster *PageBroadcaster) removeClient(client *wsClient) {
	broadcaster.mu.Lock()
	defer broadcaster.mu.Unlock()
	delete(broadcaster.clients, client)

	if len(broadcaster.clients) == 0 {
		broadcaster.shutdown()
	}

	client.close()
}

// a broadcaster is shutdown only when it does not contain any clients
// this is done to free up the redis subsciber connection
func (broadcaster *PageBroadcaster) shutdown() {
	broadcastersMu.Lock()
	defer broadcastersMu.Unlock()
	delete(broadcasters, broadcaster.pageName)
	broadcaster.cancel()
	broadcaster.sub.Close()
}
