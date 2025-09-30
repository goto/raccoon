package stats

//This package will help to send the overall statistic of the cs event
import (
	pb "buf.build/gen/go/gotocompany/proton/protocolbuffers/go/gotocompany/raccoon/v1beta1"
	"github.com/goto/raccoon/publisher"
	"google.golang.org/protobuf/types/known/timestamppb"
	"time"
)

type TotalEventStat struct {
	kp                publisher.KafkaProducer
	flushInterval     time.Duration
	topicName         string
	eventCountChannel chan int32
}

func CreateTotalEventStat(kp publisher.KafkaProducer, flushInterval time.Duration, topicName string,
	eventCountChannel chan int32) *TotalEventStat {
	return &TotalEventStat{
		kp:                kp,
		flushInterval:     flushInterval,
		topicName:         topicName,
		eventCountChannel: eventCountChannel,
	}
}

func (ts *TotalEventStat) FlushTotalEventStat() {
	ticker := time.NewTicker(ts.flushInterval)
	defer ticker.Stop()

	totalCount := int32(0)
	for {
		select {
		// flush the aggregated total event count
		case <-ticker.C:
			if totalCount > 0 {
				msg := &pb.TotalEventCountMessage{
					EventTimestamp: timestamppb.Now(),
					EventCount:     totalCount,
				}
				ts.kp.ProduceEventStat(ts.topicName, msg)
				totalCount = 0
			}
		// increment the total event count
		case cnt := <-ts.eventCountChannel:
			totalCount += cnt
		}
	}
}
