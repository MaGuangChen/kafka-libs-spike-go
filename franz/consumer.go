package franz

import (
	"context"
	"fmt"
	"math/rand"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/twmb/franz-go/pkg/kgo"
)

func NewFetcher(broker string) *kgo.Client {
	topic := viper.GetString("kafka.topic")
	fetcher, err := kgo.NewClient(
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

	return fetcher
}

func PreparePollFetch(ctx context.Context, fetcher *kgo.Client, numMessages int) func() {
	log.Infof("Preparing to receive %d messages", numMessages)
	return func() {
		var counter = 0
		for {
			fetches := fetcher.PollFetches(ctx)
			iter := fetches.RecordIter()

			for _, fetchErr := range fetches.Errors() {
				fmt.Printf("error consuming from topic: topic=%s, partition=%d, err=%v\n",
					fetchErr.Topic, fetchErr.Partition, fetchErr.Err)
				break
			}

			for !iter.Done() {
				counter++
				if counter >= numMessages {
					log.Infof("Consumed %d messages successfully...", counter)
					return
				}
			}
		}
	}
}
