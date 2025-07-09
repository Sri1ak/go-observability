package main

import (
	"bufio" // Buffered I/O package
	"context"
	"fmt" // formatted I/O to format
	"go-observability/constants"
	"go-observability/metrics"
	"hash/fnv" // hash functions
	"os"       // underlying os operations
	"os/signal"
	"strings" // string package
	"syscall"
	"time" // time package

	"github.com/confluentinc/confluent-kafka-go/kafka" // kafka package
)

type KafkaProducer interface {
	Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error
	Flush(timeoutMs int) int
	Close()
}

/*
*
function which takes service as input and takes the byte of the input string and
returns it based on the number of partitions
*
*/
func hashToPartition(service string) int32 {
	h := fnv.New32a()
	h.Write([]byte(service))
	return int32(h.Sum32() % uint32(constants.NumPartitions))
}

/*
*
Creates a new Producer with kafka(inside docker container) 19192 , 19193 as server
Reads the data from the file
Produce the data in batches with batch size 5000
Once the size is reached flush the data

*
*/
func main() {
	// call metrics Init() with 2113 to send the data to the prometheus
	metrics.Init("2113")

	/** calls adminClient of confluent kafka to produce topics
	in the bootstrap servers kafka(inside docker container) 19192,19193
	**/
	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": constants.KafkaBroker,
	})
	if err != nil {
		panic(fmt.Sprintf("Failed to create Admin client: %s", err))
	}
	defer adminClient.Close()
	ctx := context.Background()

	// Topic configuration with timeout
	results, err := adminClient.CreateTopics(
		ctx,
		[]kafka.TopicSpecification{{
			Topic:             constants.Topic,
			NumPartitions:     constants.NumPartitions,
			ReplicationFactor: constants.ReplicationFactor,
		}},
		kafka.SetAdminOperationTimeout(30*time.Second),
	)

	if err != nil {
		panic(fmt.Sprintf("CreateTopics failed: %s", err))
	}

	for _, result := range results {
		if result.Error.Code() != kafka.ErrNoError {
			fmt.Printf("Failed to create topic %s: %v\n", result.Topic, result.Error)
		} else {
			fmt.Printf("Topic %s created successfully.\n", result.Topic)
		}
	}

	// producer configs
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": constants.KafkaBroker,
	})

	if err != nil {
		fmt.Printf("Error while creating producer: %s", err)
		return
	}

	defer producer.Close()
	produceMessagesFromFile(producer, constants.InputFile)
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	<-sigchan
	fmt.Println("Exiting producer.")

}

func produceMessagesFromFile(producer KafkaProducer, filepath string) {

	// Open the input file
	file, err := os.Open(filepath)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	// reads the input file
	scanner := bufio.NewScanner(file)
	batch := []string{}
	for scanner.Scan() {
		line := scanner.Text()
		if strings.TrimSpace(line) == "" {
			continue
		}
		batch = append(batch, line)
		if len(batch) >= constants.BatchSize {
			sendBatch(batch, constants.Topic, producer)
			batch = []string{}
			time.Sleep(2 * time.Second)
		}
	}
	if len(batch) > 0 {
		sendBatch(batch, constants.Topic, producer)
	}

	// flush the data once it reaches the buffer size
	producer.Flush(5000)

}

/*
*
Sends the batch data to the topic with service name as the key and the respective partition
*
*/
func sendBatch(batch []string, topic string, p KafkaProducer) {
	for _, line := range batch {
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}
		service := parts[0]
		message := parts[1]
		partition := hashToPartition(service)
		fmt.Println("Partition name", partition)

		// produce the message with service name as key and message as value
		err := p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: partition},
			Key:            []byte(service),
			Value:          []byte(message),
		}, nil)
		if err != nil {
			fmt.Printf(" Failed to produce: %v\n", err)
		} else {
			metrics.ProducedMessages.Inc()

			fmt.Printf("Sent to %s %s (partition %d): %s\n", topic, service, partition, message)
		}
	}
}
