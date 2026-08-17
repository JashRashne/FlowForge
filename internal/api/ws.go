package api

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/flowforge/flowforge/internal/outbox"
	"github.com/redis/go-redis/v9"
)

// WebSocketHub manages active browser client WebSocket connections.
type WebSocketHub struct {
	mu      sync.RWMutex
	clients map[*websocket.Conn]struct{}
}

// NewWebSocketHub initializes a new WebSocket connection hub.
func NewWebSocketHub() *WebSocketHub {
	return &WebSocketHub{
		clients: make(map[*websocket.Conn]struct{}),
	}
}

// Register adds a connection to the active client list.
func (h *WebSocketHub) Register(conn *websocket.Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.clients[conn] = struct{}{}
}

// Unregister removes a connection from the active client list.
func (h *WebSocketHub) Unregister(conn *websocket.Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.clients, conn)
}

// Broadcast sends a message payload to all connected clients concurrently.
func (h *WebSocketHub) Broadcast(ctx context.Context, message []byte) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	for conn := range h.clients {
		writeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		err := conn.Write(writeCtx, websocket.MessageText, message)
		cancel()
		if err != nil {
			slog.Debug("failed to write to websocket client", "error", err)
			_ = conn.Close(websocket.StatusGoingAway, "write timeout")
			go h.Unregister(conn)
		}
	}
}

// ListenRedisEvents continuously polls Redis stream:events and fans out messages to WebSockets.
func (h *WebSocketHub) ListenRedisEvents(ctx context.Context, redisClient *redis.Client) {
	lastID := "$" // only read new incoming events

	for {
		select {
		case <-ctx.Done():
			return
		default:
			streams, err := redisClient.XRead(ctx, &redis.XReadArgs{
				Streams: []string{outbox.StreamEvents, lastID},
				Count:   50,
				Block:   1 * time.Second,
			}).Result()

			if err != nil {
				time.Sleep(100 * time.Millisecond)
				continue
			}

			for _, stream := range streams {
				for _, msg := range stream.Messages {
					lastID = msg.ID
					if dataStr, ok := msg.Values["data"].(string); ok {
						h.Broadcast(ctx, []byte(dataStr))
					}
				}
			}
		}
	}
}

// HandleWebSocket upgrades HTTP requests to WebSocket connections and registers them in the hub.
func (h *WebSocketHub) HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	opts := &websocket.AcceptOptions{
		InsecureSkipVerify: true, // allow all origins for local UI
	}

	conn, err := websocket.Accept(w, r, opts)
	if err != nil {
		slog.Error("failed to accept websocket connection", "error", err)
		return
	}
	defer conn.Close(websocket.StatusNormalClosure, "closing")

	h.Register(conn)
	defer h.Unregister(conn)

	// Keep connection alive until client disconnects
	ctx := r.Context()
	for {
		_, _, err := conn.Read(ctx)
		if err != nil {
			break
		}
	}
}
