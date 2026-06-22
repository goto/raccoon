package publisher

import (
	"fmt"
	"os"
	"testing"
	"time"

	pb "buf.build/gen/go/gotocompany/proton/protocolbuffers/go/gotocompany/raccoon/v1beta1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"

	"github.com/goto/raccoon/logger"
	"github.com/goto/raccoon/model"
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
		kp := NewKafkaFromClient(client, 10, map[bool]string{true: "%s", false: "%s"}, nil)
		kp.Close()
		client.AssertExpectations(t)
	})
}

func toEventsWithMetadata(events []*pb.Event) []*model.EventWithMetadata {
	res := make([]*model.EventWithMetadata, len(events))
	for i, e := range events {
		res[i] = &model.EventWithMetadata{
			Event:     e,
			EventType: e.Type,
			EventName: e.EventName,
			Product:   e.Product,
		}
	}
	return res
}

func TestKafka_ProduceBulk(suite *testing.T) {
	suite.Parallel()
	topic := "test_topic"
	testFormat := map[bool]string{true: "%s", false: "%s"}

	now := time.Now()

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
						Opaque: 0,
					}
				}()
			})
			kp := NewKafkaFromClient(client, 10, testFormat, nil)

			events := []*pb.Event{{EventBytes: []byte{}, Type: topic}, {EventBytes: []byte{}, Type: topic}}
			err := kp.ProduceBulk(toEventsWithMetadata(events), group1, make(chan kafka.Event, 2), now, now, now)
			assert.NoError(t, err)
		})

		t.Run("Should rewrite event type prefix based on config mapping", func(t *testing.T) {
			client := &mockClient{}
			events := []*pb.Event{{EventBytes: []byte{}, Type: "CS_APP_PREFIX-apihealth"}}
			client.On("Produce", mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
				message := args.Get(0).(*kafka.Message)
				assert.Equal(t, "gobiz-apihealth", *message.TopicPartition.Topic)
				go func() {
					args.Get(1).(chan kafka.Event) <- &kafka.Message{
						TopicPartition: kafka.TopicPartition{
							Topic:     message.TopicPartition.Topic,
							Partition: 0,
							Offset:    0,
							Error:     nil,
						},
						Opaque: 0,
					}
				}()
			}).Once()

			kp := NewKafkaFromClient(client, 10, testFormat, map[string]string{"CS_APP_PREFIX": "gobiz"})
			eventsWithMetadata := toEventsWithMetadata(events)
			err := kp.ProduceBulk(eventsWithMetadata, group1, make(chan kafka.Event, 1), now, now, now)

			assert.NoError(t, err)
			assert.Equal(t, "gobiz-apihealth", eventsWithMetadata[0].EventType)
		})

		t.Run("Should leave event type unchanged when mapping key does not match extracted prefix", func(t *testing.T) {
			client := &mockClient{}
			events := []*pb.Event{{EventBytes: []byte{}, Type: "CS_APP_PREFIX-apihealth"}}
			client.On("Produce", mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
				message := args.Get(0).(*kafka.Message)
				assert.Equal(t, "CS_APP_PREFIX-apihealth", *message.TopicPartition.Topic)
				go func() {
					args.Get(1).(chan kafka.Event) <- &kafka.Message{
						TopicPartition: kafka.TopicPartition{
							Topic:     message.TopicPartition.Topic,
							Partition: 0,
							Offset:    0,
							Error:     nil,
						},
						Opaque: 0,
					}
				}()
			}).Once()

			kp := NewKafkaFromClient(client, 10, testFormat, map[string]string{"CS_APP": "gobiz"})
			eventsWithMetadata := toEventsWithMetadata(events)
			err := kp.ProduceBulk(eventsWithMetadata, group1, make(chan kafka.Event, 1), now, now, now)

			assert.NoError(t, err)
			assert.Equal(t, "CS_APP_PREFIX-apihealth", eventsWithMetadata[0].EventType)
		})

		t.Run("Should leave plain event type unchanged when incoming type is page", func(t *testing.T) {
			client := &mockClient{}
			events := []*pb.Event{{EventBytes: []byte{}, Type: "page"}}
			client.On("Produce", mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
				message := args.Get(0).(*kafka.Message)
				assert.Equal(t, "page", *message.TopicPartition.Topic)
				go func() {
					args.Get(1).(chan kafka.Event) <- &kafka.Message{
						TopicPartition: kafka.TopicPartition{
							Topic:     message.TopicPartition.Topic,
							Partition: 0,
							Offset:    0,
							Error:     nil,
						},
						Opaque: 0,
					}
				}()
			}).Once()

			kp := NewKafkaFromClient(client, 10, testFormat, map[string]string{"CS_APP_PREFIX": "gobiz"})
			eventsWithMetadata := toEventsWithMetadata(events)
			err := kp.ProduceBulk(eventsWithMetadata, group1, make(chan kafka.Event, 1), now, now, now)

			assert.NoError(t, err)
			assert.Equal(t, "page", eventsWithMetadata[0].EventType)
		})

		t.Run("Should leave event type unchanged when incoming type is CS_APP without delimiter", func(t *testing.T) {
			client := &mockClient{}
			events := []*pb.Event{{EventBytes: []byte{}, Type: "CS_APP"}}
			client.On("Produce", mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
				message := args.Get(0).(*kafka.Message)
				assert.Equal(t, "CS_APP", *message.TopicPartition.Topic)
				go func() {
					args.Get(1).(chan kafka.Event) <- &kafka.Message{
						TopicPartition: kafka.TopicPartition{
							Topic:     message.TopicPartition.Topic,
							Partition: 0,
							Offset:    0,
							Error:     nil,
						},
						Opaque: 0,
					}
				}()
			}).Once()

			kp := NewKafkaFromClient(client, 10, testFormat, map[string]string{"CS_APP_PREFIX": "gobiz"})
			eventsWithMetadata := toEventsWithMetadata(events)
			err := kp.ProduceBulk(eventsWithMetadata, group1, make(chan kafka.Event, 1), now, now, now)

			assert.NoError(t, err)
			assert.Equal(t, "CS_APP", eventsWithMetadata[0].EventType)
		})

		t.Run("Should leave event type unchanged when prefix mapping is nil", func(t *testing.T) {
			client := &mockClient{}
			events := []*pb.Event{{EventBytes: []byte{}, Type: "CS_APP_PREFIX-apihealth"}}
			client.On("Produce", mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
				message := args.Get(0).(*kafka.Message)
				assert.Equal(t, "CS_APP_PREFIX-apihealth", *message.TopicPartition.Topic)
				go func() {
					args.Get(1).(chan kafka.Event) <- &kafka.Message{
						TopicPartition: kafka.TopicPartition{
							Topic:     message.TopicPartition.Topic,
							Partition: 0,
							Offset:    0,
							Error:     nil,
						},
						Opaque: 0,
					}
				}()
			}).Once()

			kp := NewKafkaFromClient(client, 10, testFormat, nil)
			eventsWithMetadata := toEventsWithMetadata(events)
			err := kp.ProduceBulk(eventsWithMetadata, group1, make(chan kafka.Event, 1), now, now, now)

			assert.NoError(t, err)
			assert.Equal(t, "CS_APP_PREFIX-apihealth", eventsWithMetadata[0].EventType)
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
						Opaque: 1,
					}
				}()
			}).Once()
			client.On("Produce", mock.Anything, mock.Anything).Return(fmt.Errorf("buffer full")).Once()
			kp := NewKafkaFromClient(client, 10, testFormat, nil)

			events := []*pb.Event{{EventBytes: []byte{}, Type: topic}, {EventBytes: []byte{}, Type: topic}, {EventBytes: []byte{}, Type: topic}}
			err := kp.ProduceBulk(toEventsWithMetadata(events), group1, make(chan kafka.Event, 2), now, now, now)
			assert.Len(t, err.(BulkError).Errors, 3)
			assert.Error(t, err.(BulkError).Errors[0])
			assert.Empty(t, err.(BulkError).Errors[1])
			assert.Error(t, err.(BulkError).Errors[2])
		})

		t.Run("Should return topic name when unknown topic is returned", func(t *testing.T) {
			client := &mockClient{}
			client.On("Produce", mock.Anything, mock.Anything).Return(fmt.Errorf(errUnknownTopic)).Once()
			kp := NewKafkaFromClient(client, 10, testFormat, nil)

			events := []*pb.Event{{EventBytes: []byte{}, Type: topic}}
			err := kp.ProduceBulk(toEventsWithMetadata(events), "group1", make(chan kafka.Event, 2), now, now, now)
			assert.EqualError(t, err.(BulkError).Errors[0], errUnknownTopic+" "+topic)
		})

		t.Run("Should return topic name when message size is too large", func(t *testing.T) {
			client := &mockClient{}
			client.On("Produce", mock.Anything, mock.Anything).Return(fmt.Errorf(errLargeMessageSize)).Once()
			kp := NewKafkaFromClient(client, 10, testFormat, nil)

			events := []*pb.Event{{EventBytes: []byte{}, Type: topic}}
			err := kp.ProduceBulk(toEventsWithMetadata(events), "group1", make(chan kafka.Event, 2), now, now, now)
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
			kp := NewKafkaFromClient(client, 10, testFormat, nil)

			events := []*pb.Event{{EventBytes: []byte{}, Type: topic}, {EventBytes: []byte{}, Type: topic}}
			err := kp.ProduceBulk(toEventsWithMetadata(events), "group1", make(chan kafka.Event, 2), now, now, now)
			assert.NotEmpty(t, err)
			assert.Len(t, err.(BulkError).Errors, 2)
			assert.Equal(t, "buffer full", err.(BulkError).Errors[0].Error())
			assert.Equal(t, "timeout", err.(BulkError).Errors[1].Error())
		})
	})
}
