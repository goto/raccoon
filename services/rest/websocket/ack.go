package websocket

import (
	"context"
	"github.com/goto/raccoon/logger"
	"time"

	"github.com/goto/raccoon/metrics"
	"github.com/goto/raccoon/serialization"
	"github.com/goto/raccoon/services/rest/websocket/connection"
)

var AckChan = make(chan AckInfo)

type AckInfo struct {
	MessageType     int
	RequestGuid     string
	Err             error
	Conn            connection.Conn
	serializer      serialization.SerializeFunc
	TimeConsumed    time.Time
	AckTimeConsumed time.Time
}

func AckHandler(ctx context.Context, ch <-chan AckInfo) {
	for {
		select {
		case c := <-ch:
			ackTim := time.Since(c.AckTimeConsumed)
			metrics.Timing("ack_event_rtt_ms", ackTim.Milliseconds(), "")

			tim := time.Since(c.TimeConsumed)
			if c.Err != nil {
				metrics.Timing("event_rtt_ms", tim.Milliseconds(), "")
				writeFailedResponse(c.Conn, c.serializer, c.MessageType, c.RequestGuid, c.Err)
				continue
			}

			metrics.Timing("event_rtt_ms", tim.Milliseconds(), "")
			writeSuccessResponse(c.Conn, c.serializer, c.MessageType, c.RequestGuid)

		case <-ctx.Done():
			// graceful shutdown
			logger.Info("[AckHandler] - stopping ack handler exiting")
			return
		}
	}
}
