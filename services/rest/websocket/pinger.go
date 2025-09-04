package websocket

import (
	"context"
	"fmt"
	"time"

	"github.com/goto/raccoon/identification"
	"github.com/goto/raccoon/logger"
	"github.com/goto/raccoon/metrics"
	"github.com/goto/raccoon/services/rest/websocket/connection"
)

// Pinger is worker that pings the connected peers based on ping interval.
func Pinger(ctx context.Context, c chan connection.Conn, size int, PingInterval time.Duration, WriteWaitInterval time.Duration) {
	//shutdown the pinger
	for i := 0; i < size; i++ {
		go func() {
			cSet := make(map[identification.Identifier]connection.Conn)
			ticker := time.NewTicker(PingInterval)
			defer ticker.Stop()
			for {
				select {
				//close the connection
				case <-ctx.Done():
					// shutdown signal received
					logger.Infof("[websocket.pinger] shutting down, closing %d active connections", len(cSet))
					for _, conn := range cSet {
						conn.Close()
					}
					logger.Info("[websocket.pinger] - shutting down, successful")
					return
				case conn := <-c:
					cSet[conn.Identifier] = conn
				case <-ticker.C:
					for identifier, conn := range cSet {
						logger.Debug(fmt.Sprintf("Pinging %s ", identifier))
						if err := conn.Ping(WriteWaitInterval); err != nil {
							logger.Error(fmt.Sprintf("[websocket.pingPeer] - Failed to ping %s: %v", identifier, err))
							metrics.Increment("server_ping_failure_total", fmt.Sprintf("conn_group=%s", identifier.Group))
							delete(cSet, identifier)
						}
					}
				}
			}
		}()
	}
}
