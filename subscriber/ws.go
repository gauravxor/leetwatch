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
	connection *websocket.Conn // the websocket connection
	pageName   string          // the page viewed through websocket connection
	closeOnce  sync.Once
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
	if !isRedisHealthy() {
		log.Println("Redis unhealthy")
		http.Error(resp, "Service unavailable", http.StatusServiceUnavailable)
		return
	}

	wsConnection, err := upgrader.Upgrade(resp, req, nil)
	if err != nil {
		log.Println("Error upgrading WS connection: ", err)
		return
	}

	log.Println("Client connected.")

	pageName := strings.TrimPrefix(req.URL.Path, "/ws/")
	countKey := countKeyPrefix + pageName

	// increase the viewer's count for the given page
	updatedCount, err := redisClient.Incr(ctx, countKey).Result()
	log.Printf("Updated viewers = %d", updatedCount)
	if err != nil {
		log.Println("Redis INCR error:", err)
		wsConnection.Close()
		return
	}

	// immediately send the viewer's count to the connected client
	wsConnection.WriteMessage(websocket.TextMessage, fmt.Appendf(nil, "%d", updatedCount))

	client := &wsClient{
		connection: wsConnection,
		pageName:   pageName,
	}

	activeClients.Store(client, struct{}{})

	broadcaster := getOrCreateBroadcaster(pageName)
	broadcaster.addClient(client)

	go monitorClientDisconnection(client, broadcaster)
}

func (client *wsClient) close() {
	log.Println("Closing/cleaning up WS connection")

	client.closeOnce.Do(func() {
		activeClients.Delete(client)

		countKey := countKeyPrefix + client.pageName

		// decrement the view counter
		_, err := redisClient.Decr(ctx, countKey).Result()
		if err != nil {
			// TODO: Handle stale values
			log.Println("Redis DECR error:", err)
		}

		client.connection.Close()
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
		if _, _, err := client.connection.ReadMessage(); err != nil {
			broadcaster.removeClient(client)
			return
		}
	}
}

func getOrCreateBroadcaster(pageName string) *PageBroadcaster {
	broadcastersMu.Lock()
	defer broadcastersMu.Unlock()

	// if existing broadcaster is there for the given page, return it.
	if existing, ok := broadcasters[pageName]; ok {
		return existing
	}

	// if not, then create one
	ctx, cancel := context.WithCancel(context.Background())
	redisSub := redisClient.Subscribe(ctx, channelPrefix+pageName)

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
			client.connection.WriteMessage(websocket.TextMessage, []byte(msg.Payload))
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
