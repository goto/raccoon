package websocket

import (
	"fmt"
	"raccoon/logger"
	"raccoon/metrics"
	"time"

	"github.com/gorilla/websocket"
)

type connection struct {
	userID string
	conn   *websocket.Conn
}

//Pinger is a worker groroutine that pings the connected peers based on ping interval.
func Pinger(c chan connection, size int, PingInterval time.Duration, WriteWaitInterval time.Duration) {
	for i := 0; i < size; i++ {
		go func() {
			cSet := make(map[string]*websocket.Conn)
			timer := time.NewTicker(PingInterval)
			for {
				select {
				case conn := <-c:
					cSet[conn.userID] = conn.conn
				case <-timer.C:
					for userID, conn := range cSet {
						logger.Debug(fmt.Sprintf("Pinging UserId: %s ", userID))
						if err := conn.WriteControl(websocket.PingMessage, []byte("--ping--"), time.Now().Add(WriteWaitInterval)); err != nil {
							logger.Error(fmt.Sprintf("[websocket.pingPeer] - Failed to ping User: %s Error: %v", userID, err))
							metrics.Count("server.ping.failed", 1, "")
							delete(cSet, userID)
						}
					}
				}
			}
		}()
	}
}