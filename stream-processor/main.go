package main

import (
	"fmt" // formatted I/O
	"go-observability/constants"
	"go-observability/metrics"
	"os"
	"os/signal" // signal opertions from os

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type KafkaConsumer interface {
	SubscribeTopics(topics []string, rebalanceCb kafka.RebalanceCb) error
	Poll(timeoutMs int) kafka.Event
	Close() error
}

/*
*
Creates a new Consumer
Poll the topic for the message and reads it whenever there is a message
Termainates on interruption
*
*/
func main() {
	// call metrics Init() with port 2114 to send the data to the prometheus
	metrics.Init("2114")

	//creates the consumer
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": constants.KafkaBroker,
		"group.id":          constants.GroupID,
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		panic(err)
	}
	defer consumer.Close()
	sigchan := make(chan os.Signal, 1)
	// notifies the terminate operation
	signal.Notify(sigchan, os.Interrupt)

	// subscribes to the list of topics
	consumeMessage(consumer, sigchan)

}
func consumeMessage(consumer KafkaConsumer, sigchan chan os.Signal) {

	err := consumer.SubscribeTopics([]string{constants.Topic}, nil)
	if err != nil {
		panic(err)
	}
	fmt.Println("Consumer waiting for the messages", constants.Topic)

	run := true

	for run {
		select {
		case sig := <-sigchan:
			fmt.Printf("Terminating consumer %v\n", sig) // terminates the consumer on interupts
			run = false
		default:
			ev := consumer.Poll(100) // consumer polls kafka for any new event every 100ms
			// ignore null event
			if ev == nil {
				continue
			}
			switch e := ev.(type) {
			case *kafka.Message:
				service := string(e.Key)
				metrics.LogsConsumedTotal.WithLabelValues(service).Inc()

				// Process the consumed message
				fmt.Printf("Received message from topic %s: %d: %s\n", *e.TopicPartition.Topic, e.TopicPartition.Partition, string(e.Value))
			case kafka.Error:
				// Handle Kafka errors
				fmt.Printf("Error: %v\n", e)
			}
		}
	}

}
