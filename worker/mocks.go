package worker

import (
	pb "buf.build/gen/go/gotocompany/proton/protocolbuffers/go/gotocompany/raccoon/v1beta1"
	mock "github.com/stretchr/testify/mock"
	kafka "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

// KafkaProducer is an autogenerated mock type for the KafkaProducer type
type mockKafkaPublisher struct {
	mock.Mock
}

// ProduceBulk provides a mock function with given fields: events, deliveryChannel
func (m *mockKafkaPublisher) ProduceBulk(events []*pb.Event, connGroup string, deliveryChannel chan kafka.Event) error {
	mock := m.Called(events, connGroup, deliveryChannel)
	return mock.Error(0)
}

type mockMetric struct {
	mock.Mock
}

func (m *mockMetric) Count(bucket string, val int, tags string) {
	m.Called(bucket, val, tags)
}

func (m *mockMetric) Timing(bucket string, t int64, tags string) {
	m.Called(bucket, t, tags)
}
