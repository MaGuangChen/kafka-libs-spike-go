package franz

import (
	"context"
	"fmt"
	"math/rand"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/twmb/franz-go/pkg/kgo"
)

func NewProducer(broker string) *kgo.Client {
	topic := viper.GetString("kafka.topic")
	producer, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()), // only read messages that have been written as part of committed transactions
		kgo.ConsumerGroup(fmt.Sprintf("group-%d", rand.Intn(10000))),
		kgo.ConsumeTopics(topic),
		kgo.AllowedConcurrentFetches(100),
		kgo.MaxBufferedRecords(250<<20/1000+1),
		kgo.AllowedConcurrentFetches(100),
		kgo.FetchMaxBytes(10e6),
		kgo.BatchMaxBytes(int32(1000000)),
	)

	if err != nil {
		log.WithError(err).Panic("Unable to start the consumer")
	}

	return producer
}

func Prepare(producer *kgo.Client, message []byte, numMessages int) func() {
	log.Infof("Preparing to send message of %d bytes %d times", len(message), numMessages)

	return func() {
		topic := viper.GetString("kafka.topic")
		for j := 0; j < numMessages; j++ {
			record := &kgo.Record{Topic: topic, Value: message}
			// var wg sync.WaitGroup
			// wg.Add(1)
			// fmt.Println("record: ", record)
			producer.Produce(context.Background(), record, func(_ *kgo.Record, err error) {
				// defer wg.Done()
				if err != nil {
					fmt.Printf("record had a produce error: %v\n", err)
				}
			})
			// wg.Wait()
		}
	}
}
