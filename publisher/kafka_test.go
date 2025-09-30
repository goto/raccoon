package publisher

import (
	pb "buf.build/gen/go/gotocompany/proton/protocolbuffers/go/gotocompany/raccoon/v1beta1"
	"fmt"
	"github.com/goto/raccoon/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"os"
	"testing"
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

func TestProducer_Close(suite *testing.T) {
	suite.Run("Should flush before closing the client", func(t *testing.T) {
		client := &mockClient{}
		client.On("Flush", 10).Return(0)
		client.On("Close").Return()
		kp := NewKafkaFromClient(client, 10, "%s", make(chan int32, 1))
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
			eventCountCh := make(chan int32, 10)
			kp := NewKafkaFromClient(client, 10, "%s", eventCountCh)

			err := kp.ProduceBulk([]*pb.Event{{EventBytes: []byte{}, Type: topic}, {EventBytes: []byte{}, Type: topic}}, group1, make(chan kafka.Event, 2))
			assert.NoError(t, err)
			expectedEventCount := int32(2)
			actualEventCount := int32(0)
			select {
			case v := <-eventCountCh:
				actualEventCount += v
			default:
			}
			assert.Equal(t, expectedEventCount, actualEventCount)
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
			eventCountCh := make(chan int32, 10)
			kp := NewKafkaFromClient(client, 10, "%s", eventCountCh)

			err := kp.ProduceBulk([]*pb.Event{{EventBytes: []byte{}, Type: topic}, {EventBytes: []byte{}, Type: topic}, {EventBytes: []byte{}, Type: topic}}, group1, make(chan kafka.Event, 2))
			assert.Len(t, err.(BulkError).Errors, 3)
			assert.Error(t, err.(BulkError).Errors[0])
			assert.Empty(t, err.(BulkError).Errors[1])
			assert.Error(t, err.(BulkError).Errors[2])

			expectedEventCount := int32(1)
			actualEventCount := int32(0)
			select {
			case v := <-eventCountCh:
				actualEventCount += v
			default:
			}
			assert.Equal(t, expectedEventCount, actualEventCount)
		})

		t.Run("Should return topic name when unknown topic is returned", func(t *testing.T) {
			client := &mockClient{}
			client.On("Produce", mock.Anything, mock.Anything).Return(fmt.Errorf(errUnknownTopic)).Once()
			eventCountCh := make(chan int32, 10)
			kp := NewKafkaFromClient(client, 10, "%s", eventCountCh)

			err := kp.ProduceBulk([]*pb.Event{{EventBytes: []byte{}, Type: topic}}, "group1", make(chan kafka.Event, 2))
			assert.EqualError(t, err.(BulkError).Errors[0], errUnknownTopic+" "+topic)

			expectedEventCount := int32(0)
			actualEventCount := int32(0)
			select {
			case v := <-eventCountCh:
				actualEventCount += v
			default:
			}
			assert.Equal(t, expectedEventCount, actualEventCount)
		})

		t.Run("Should return topic name when message size is too large", func(t *testing.T) {
			client := &mockClient{}
			client.On("Produce", mock.Anything, mock.Anything).Return(fmt.Errorf(errLargeMessageSize)).Once()
			eventCountCh := make(chan int32, 10)
			kp := NewKafkaFromClient(client, 10, "%s", eventCountCh)

			err := kp.ProduceBulk([]*pb.Event{{EventBytes: []byte{}, Type: topic}}, "group1", make(chan kafka.Event, 2))
			assert.EqualError(t, err.(BulkError).Errors[0], errLargeMessageSize+" "+topic)

			expectedEventCount := int32(0)
			actualEventCount := int32(0)
			select {
			case v := <-eventCountCh:
				actualEventCount += v
			default:
			}
			assert.Equal(t, expectedEventCount, actualEventCount)
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
			eventCountCh := make(chan int32, 10)
			kp := NewKafkaFromClient(client, 10, "%s", eventCountCh)

			err := kp.ProduceBulk([]*pb.Event{{EventBytes: []byte{}, Type: topic}, {EventBytes: []byte{}, Type: topic}}, "group1", make(chan kafka.Event, 2))
			assert.NotEmpty(t, err)
			assert.Len(t, err.(BulkError).Errors, 2)
			assert.Equal(t, "buffer full", err.(BulkError).Errors[0].Error())
			assert.Equal(t, "timeout", err.(BulkError).Errors[1].Error())

			expectedEventCount := int32(0)
			actualEventCount := int32(0)
			select {
			case v := <-eventCountCh:
				actualEventCount += v
			default:
			}
			assert.Equal(t, expectedEventCount, actualEventCount)
		})
	})
}

func TestKafka_ProduceEventStat(t *testing.T) {
	topic := "stats_topic"

	t.Run("Should serialize and produce successfully", func(t *testing.T) {
		client := &mockClient{}
		client.On("Produce", mock.Anything, mock.Anything).Return(nil).Once()

		eventCountCh := make(chan int32, 1)
		kp := NewKafkaFromClient(client, 10, "%s", eventCountCh)

		msg := &pb.TotalEventCountMessage{
			EventCount:     42,
			EventTimestamp: timestamppb.Now(),
		}

		err := kp.ProduceEventStat(topic, msg)
		assert.NoError(t, err)

		// Verify Produce was called
		client.AssertExpectations(t)
	})

}
