package kafkago

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// NewProducer returns a new kafkago writer.
func NewProducer(brokers string) *kafkago.Writer {
	conn, err := kafkago.Dial("tcp", brokers)
	if err != nil {
		panic(err.Error())
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		panic(err.Error())
	}
	var controllerConn *kafkago.Conn
	controllerConn, err = kafkago.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		panic(err.Error())
	}
	defer controllerConn.Close()
	topicConfigs := []kafkago.TopicConfig{
		kafkago.TopicConfig{
			Topic:             viper.GetString("kafka.topic"),
			NumPartitions:     1,
			ReplicationFactor: 1,
		},
	}

	err = controllerConn.CreateTopics(topicConfigs...)
	if err != nil {
		panic(err.Error())
	}

	writer := kafkago.NewWriter(kafkago.WriterConfig{
		Brokers:       []string{brokers},
		Topic:         viper.GetString("kafka.topic"),
		Balancer:      &kafkago.Hash{},
		BatchTimeout:  time.Duration(100) * time.Millisecond,
		QueueCapacity: 10000,
		BatchSize:     1000000,
		// Async doesn't allow us to know if message has been successfully sent to Kafka.
		// Async:         true,
	})

	return writer
}

// Prepare returns a function that can be used during the benchmark as it only
// performs the sending of messages.
func Prepare(writer *kafkago.Writer, message []byte, numMessages int) func() {
	log.Infof("Preparing to send message of %d bytes %d times", len(message), numMessages)
	fmt.Println("this is writer: ", writer)

	return func() {
		for j := 0; j < numMessages; j++ {
			err := writer.WriteMessages(context.Background(), kafkago.Message{Value: message})
			if err != nil {
				log.WithError(err).Panic("Unable to deliver the message")
			}
		}
	}
}
