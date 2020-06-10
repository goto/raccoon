package publisher

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"os"
	"os/signal"
	"raccoon/config"
	"raccoon/logger"
	"syscall"
)

func newProducer(kp KafkaProducer, config config.KafkaConfig) *Producer {
	return &Producer{
		kp:     kp,
		Config: config,
	}
}

type Producer struct {
	kp               KafkaProducer
	Config           config.KafkaConfig
}

func (pr *Producer) produce(msg *kafka.Message, deliveryChannel chan kafka.Event) error {

	produceErr := pr.kp.Produce(msg, deliveryChannel)

	if produceErr != nil {
		logger.Error("Kafka producer creation failed", produceErr)
		return produceErr
	}

	e := <-deliveryChannel
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		logger.Error(fmt.Sprintf("Kafka message delivery failed.%s", m.TopicPartition.Error))
		return m.TopicPartition.Error
	} else {
		logger.Debug(fmt.Sprintf("Delivered message to topic %s [%d] at offset %s",
			*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset))
	}
	close(deliveryChannel)
	return nil
}

func (pr *Producer) Close() {
	pr.kp.Close()
}

func (pr *Producer) Flush(flushInterval int) {
	 pr.kp.Flush(flushInterval)
}

func ShutdownProducer(ctx context.Context, pr *Producer) {
	signalChan := make(chan os.Signal)
	signal.Notify(signalChan, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	for {
		sig := <-signalChan
		switch sig {
		case syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT:
			logger.Debug(fmt.Sprintf("[Kafka.Producer] Received a signal %s", sig))
			logger.Debug("Closing Producer")
			pr.Close()
			flushInterval := config.NewKafkaConfig().FlushInterval()
			pr.Flush(flushInterval)
		default:
			logger.Error(fmt.Sprintf("[Kafka.Producer] Received a unexpected signal %s", sig))
		}
	}
}
