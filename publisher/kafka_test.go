package publisher

import (
	"fmt"
	"github.com/goto/raccoon/deserialization"
	"github.com/goto/raccoon/proto"
	"os"
	"sync/atomic"
	"testing"
	"time"

	pb "buf.build/gen/go/gotocompany/proton/protocolbuffers/go/gotocompany/raccoon/v1beta1"
	"github.com/goto/raccoon/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

const (
	group1 = "group-1"
)

type void struct{}

func (v void) Write(_ []byte) (int, error) {
	return 0, nil
}
func TestMain(t *testing.M) {
	logger.SetOutput(void{})
	os.Exit(t.Run())
}

var (
	deliveryReportInterval = 1 * time.Millisecond
	deliveryReportTopic    = "clickstream-test-log"
)

func TestProducer_Close(suite *testing.T) {
	suite.Run("Should flush before closing the client", func(t *testing.T) {
		client := &mockClient{}
		client.On("Flush", 10).Return(0)
		client.On("Close").Return()
		kp := NewKafkaFromClient(client, 10, "%s", deliveryReportInterval, deliveryReportTopic)
		kp.Close()
		client.AssertExpectations(t)
	})
}

func TestKafka_ProduceBulk(suite *testing.T) {
	suite.Parallel()
	topic := "test_topic"
	suite.Run("AllMessagesSuccessfulProduce", func(t *testing.T) {
		t.Run("Should return nil when all message succesfully published", func(t *testing.T) {
			client := &mockClient{}
			client.On("Produce", mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
				go func() {
					args.Get(1).(chan kafka.Event) <- &kafka.Message{
						TopicPartition: kafka.TopicPartition{
							Topic:     args.Get(0).(*kafka.Message).TopicPartition.Topic,
							Partition: 0,
							Offset:    0,
							Error:     nil,
						},
					}
				}()
			})
			kp := NewKafkaFromClient(client, 10, "%s", deliveryReportInterval, deliveryReportTopic)
			atomic.StoreInt64(&DeliveryEventCount, 0) //reset the value to 0 before call the produce function
			err := kp.ProduceBulk([]*pb.Event{{EventBytes: []byte{}, Type: topic}, {EventBytes: []byte{}, Type: topic}}, group1, make(chan kafka.Event, 2))
			assert.NoError(t, err)
			assert.Equal(t, int64(2), atomic.LoadInt64(&DeliveryEventCount)) //two events are sent to produce bulk
		})
	})

	suite.Run("PartialSuccessfulProduce", func(t *testing.T) {
		t.Run("Should process non producer error messages", func(t *testing.T) {
			client := &mockClient{}
			client.On("Produce", mock.Anything, mock.Anything).Return(fmt.Errorf("buffer full")).Once()
			client.On("Produce", mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
				go func() {
					args.Get(1).(chan kafka.Event) <- &kafka.Message{
						TopicPartition: kafka.TopicPartition{
							Topic:     args.Get(0).(*kafka.Message).TopicPartition.Topic,
							Partition: 0,
							Offset:    0,
							Error:     nil,
						},
					}
				}()
			}).Once()
			client.On("Produce", mock.Anything, mock.Anything).Return(fmt.Errorf("buffer full")).Once()
			kp := NewKafkaFromClient(client, 10, "%s", deliveryReportInterval, deliveryReportTopic)
			atomic.StoreInt64(&DeliveryEventCount, 0) //reset the value to 0 before call the produce function
			err := kp.ProduceBulk([]*pb.Event{{EventBytes: []byte{}, Type: topic}, {EventBytes: []byte{}, Type: topic}, {EventBytes: []byte{}, Type: topic}}, group1, make(chan kafka.Event, 2))
			assert.Len(t, err.(BulkError).Errors, 3)
			assert.Error(t, err.(BulkError).Errors[0])
			assert.Empty(t, err.(BulkError).Errors[1])
			assert.Error(t, err.(BulkError).Errors[2])
			assert.Equal(t, int64(1), atomic.LoadInt64(&DeliveryEventCount)) // only one message has been successfully produced
		})

		t.Run("Should return topic name when unknown topic is returned", func(t *testing.T) {
			client := &mockClient{}
			client.On("Produce", mock.Anything, mock.Anything).Return(fmt.Errorf(errUnknownTopic)).Once()
			kp := NewKafkaFromClient(client, 10, "%s", deliveryReportInterval, deliveryReportTopic)
			atomic.StoreInt64(&DeliveryEventCount, 0) //reset the value to 0 before call the produce function
			err := kp.ProduceBulk([]*pb.Event{{EventBytes: []byte{}, Type: topic}}, "group1", make(chan kafka.Event, 2))
			assert.EqualError(t, err.(BulkError).Errors[0], errUnknownTopic+" "+topic)
			assert.Equal(t, int64(0), atomic.LoadInt64(&DeliveryEventCount)) // no message has been successfully produced
		})

		t.Run("Should return topic name when message size is too large", func(t *testing.T) {
			client := &mockClient{}
			client.On("Produce", mock.Anything, mock.Anything).Return(fmt.Errorf(errLargeMessageSize)).Once()
			kp := NewKafkaFromClient(client, 10, "%s", deliveryReportInterval, deliveryReportTopic)
			atomic.StoreInt64(&DeliveryEventCount, 0) //reset the value to 0 before call the produce function
			err := kp.ProduceBulk([]*pb.Event{{EventBytes: []byte{}, Type: topic}}, "group1", make(chan kafka.Event, 2))
			assert.EqualError(t, err.(BulkError).Errors[0], errLargeMessageSize+" "+topic)
			assert.Equal(t, int64(0), atomic.LoadInt64(&DeliveryEventCount)) // no message has been successfully produced
		})
	})

	suite.Run("MessageFailedToProduce", func(t *testing.T) {
		t.Run("Should fill all errors when all messages fail", func(t *testing.T) {
			client := &mockClient{}
			client.On("Produce", mock.Anything, mock.Anything).Return(fmt.Errorf("buffer full")).Once()
			client.On("Produce", mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
				go func() {
					args.Get(1).(chan kafka.Event) <- &kafka.Message{
						TopicPartition: kafka.TopicPartition{
							Topic:     args.Get(0).(*kafka.Message).TopicPartition.Topic,
							Partition: 0,
							Offset:    0,
							Error:     fmt.Errorf("timeout"),
						},
						Opaque: 1,
					}
				}()
			}).Once()
			kp := NewKafkaFromClient(client, 10, "%s", deliveryReportInterval, deliveryReportTopic)
			atomic.StoreInt64(&DeliveryEventCount, 0) //reset the value to 0 before call the produce function
			err := kp.ProduceBulk([]*pb.Event{{EventBytes: []byte{}, Type: topic}, {EventBytes: []byte{}, Type: topic}}, "group1", make(chan kafka.Event, 2))
			assert.NotEmpty(t, err)
			assert.Len(t, err.(BulkError).Errors, 2)
			assert.Equal(t, "buffer full", err.(BulkError).Errors[0].Error())
			assert.Equal(t, "timeout", err.(BulkError).Errors[1].Error())
			assert.Equal(t, int64(0), atomic.LoadInt64(&DeliveryEventCount)) // no message has been successfully produced
		})
	})
}

func TestKafka_ReportDeliveryEventCount(suite *testing.T) {
	suite.Run("Should Produce the delivery count and reset it ", func(t *testing.T) {
		client := &mockClient{}
		producedCh := make(chan *proto.TotalEventCountMessage, 1)

		client.On("Produce", mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
			msg := args.Get(0).(*kafka.Message)
			var produced proto.TotalEventCountMessage
			err := deserialization.DeserializeProto(msg.Value, &produced) // use protobuf unmarshal
			assert.NoError(t, err)
			producedCh <- &produced
		})

		kp := NewKafkaFromClient(client, 10, "%s", 1*time.Millisecond, deliveryReportTopic)

		atomic.StoreInt64(&DeliveryEventCount, 5)

		go kp.ReportDeliveryEventCount()

		// Wait for the message instead of sleeping
		select {
		case produced := <-producedCh:
			assert.Equal(t, int32(5), produced.EventCount)
		case <-time.After(10 * time.Millisecond):
			t.Fatal("timeout: did not receive produced message")
		}

		// After one tick, counter should be reset
		assert.Equal(t, int64(0), atomic.LoadInt64(&DeliveryEventCount))
		client.AssertExpectations(t)
	})

}
