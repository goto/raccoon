package publisher

import (
	pb "buf.build/gen/go/gotocompany/proton/protocolbuffers/go/gotocompany/raccoon/v1beta1"
	"fmt"
	"github.com/goto/raccoon/deserialization"
	"github.com/goto/raccoon/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
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

var (
	clickstreamStats = ClickstreamStats{
		topicName: "clickstream-test-log",
	}
)

func TestProducer_Close(suite *testing.T) {
	suite.Run("Should flush before closing the client", func(t *testing.T) {
		client := &mockClient{}
		client.On("Flush", 10).Return(0)
		client.On("Close").Return()
		kp := NewKafkaFromClient(client, 10, "%s", clickstreamStats)
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
			kp := NewKafkaFromClient(client, 10, "%s", clickstreamStats)
			err := kp.ProduceBulk([]*pb.Event{{EventBytes: []byte{}, Type: topic}, {EventBytes: []byte{}, Type: topic}}, group1, make(chan kafka.Event, 2))
			assert.NoError(t, err)
		})
		t.Run("Should return nil when all message successfully published and stats message sent", func(t *testing.T) {
			client := &mockClient{}

			// For normal events -> always succeed and simulate delivery
			client.On("Produce", mock.MatchedBy(func(msg *kafka.Message) bool {
				// filter out stats messages (they'll be handled separately)
				return msg.TopicPartition.Topic != nil && *msg.TopicPartition.Topic == topic
			}), mock.Anything).Return(nil).Run(func(args mock.Arguments) {
				go func() {
					args.Get(1).(chan kafka.Event) <- &kafka.Message{
						TopicPartition: kafka.TopicPartition{
							Topic:     args.Get(0).(*kafka.Message).TopicPartition.Topic,
							Partition: 0,
							Offset:    0,
							Error:     nil,
						},
						Opaque: 0,
					}
				}()
			})

			// For stats message -> match by topic and decode value
			client.On("Produce", mock.MatchedBy(func(msg *kafka.Message) bool {
				return msg.TopicPartition.Topic != nil && *msg.TopicPartition.Topic == clickstreamStats.topicName
			}), mock.Anything).Return(nil).Run(func(args mock.Arguments) {
				// decode the protobuf payload into TotalEventCountMessage
				statsMsg := &pb.TotalEventCountMessage{}
				err := deserialization.DeserializeProto(args.Get(0).(*kafka.Message).Value, statsMsg)
				assert.NoError(t, err)
				assert.EqualValues(t, 2, statsMsg.EventCount) // we sent 2 events
			})

			kp := NewKafkaFromClient(client, 10, "%s", clickstreamStats)

			err := kp.ProduceBulk(
				[]*pb.Event{
					{EventBytes: []byte("foo"), Type: topic},
					{EventBytes: []byte("bar"), Type: topic},
				},
				group1,
				make(chan kafka.Event, 2),
			)

			assert.NoError(t, err)

			// Assert both normal and stats produce were called
			client.AssertNumberOfCalls(t, "Produce", 3) // 2 events + 1 stats
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
			client.On("Produce", mock.Anything, mock.Anything).Return(nil).Once()
			kp := NewKafkaFromClient(client, 10, "%s", clickstreamStats)
			err := kp.ProduceBulk([]*pb.Event{{EventBytes: []byte{}, Type: topic}, {EventBytes: []byte{}, Type: topic}, {EventBytes: []byte{}, Type: topic}}, group1, make(chan kafka.Event, 2))
			assert.Len(t, err.(BulkError).Errors, 3)
			assert.Error(t, err.(BulkError).Errors[0])
			assert.Empty(t, err.(BulkError).Errors[1])
			assert.Error(t, err.(BulkError).Errors[2])
		})

		t.Run("Should return topic name when unknown topic is returned", func(t *testing.T) {
			client := &mockClient{}
			client.On("Produce", mock.Anything, mock.Anything).Return(fmt.Errorf(errUnknownTopic))
			kp := NewKafkaFromClient(client, 10, "%s", clickstreamStats)
			err := kp.ProduceBulk([]*pb.Event{{EventBytes: []byte{}, Type: topic}}, "group1", make(chan kafka.Event, 2))
			assert.EqualError(t, err.(BulkError).Errors[0], errUnknownTopic+" "+topic)
		})

		t.Run("Should return topic name when message size is too large", func(t *testing.T) {
			client := &mockClient{}
			client.On("Produce", mock.Anything, mock.Anything).Return(fmt.Errorf(errLargeMessageSize)).Once()
			kp := NewKafkaFromClient(client, 10, "%s", clickstreamStats)
			err := kp.ProduceBulk([]*pb.Event{{EventBytes: []byte{}, Type: topic}}, "group1", make(chan kafka.Event, 2))
			assert.EqualError(t, err.(BulkError).Errors[0], errLargeMessageSize+" "+topic)
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
			kp := NewKafkaFromClient(client, 10, "%s", clickstreamStats)
			err := kp.ProduceBulk([]*pb.Event{{EventBytes: []byte{}, Type: topic}, {EventBytes: []byte{}, Type: topic}}, "group1", make(chan kafka.Event, 2))
			assert.NotEmpty(t, err)
			assert.Len(t, err.(BulkError).Errors, 2)
			assert.Equal(t, "buffer full", err.(BulkError).Errors[0].Error())
			assert.Equal(t, "timeout", err.(BulkError).Errors[1].Error())
		})
	})
}
