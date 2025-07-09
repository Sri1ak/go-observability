package main

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type MockConsumer struct {
	mock.Mock
}

func (m *MockConsumer) SubscribeTopics(topics []string, rebalanceCb kafka.RebalanceCb) error {
	args := m.Called(topics, rebalanceCb)
	return args.Error(0)
}
func nilEvent() kafka.Event {
	return nil
}

func (m *MockConsumer) Poll(timeoutMs int) kafka.Event {
	args := m.Called(timeoutMs)
	if args.Get(0) == nil {
		return nil
	}
	return args.Get(0).(kafka.Event)
}

func (m *MockConsumer) Close() error {
	m.Called()
	return nil
}

func Test_Stream_Processor(t *testing.T) {

	mockConsumer := new(MockConsumer)

	mockConsumer.On("SubscribeTopics", mock.Anything, mock.Anything).Return(nil)
	mockConsumer.On("Poll", mock.Anything).Return(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &[]string{"stream-Topic"}[0], Partition: 0},
		Value:          []byte("test-message"),
	}).Once()
	mockConsumer.On("Close").Return(nil)
}

func TestConsumeMessages(t *testing.T) {
	mockConsumer := new(MockConsumer)
	stopChan := make(chan os.Signal, 1)

	// Expect SubscribeTopics call
	mockConsumer.On("SubscribeTopics", []string{"stream-Topic"}, mock.Anything).Return(nil)

	// Setup Poll to first return a message
	mockConsumer.On("Poll", mock.Anything).Return(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &[]string{"stream-Topic"}[0], Partition: 0},
		Value:          []byte("test-message"),
	}).Once()

	mockConsumer.On("Poll", mock.Anything).Return(nilEvent()).Maybe()

	// Stop the consumer after some time to avoid infinite loop
	go func() {
		fmt.Println("Sending interrupt signal")
		time.Sleep(10 * time.Second)
		stopChan <- os.Interrupt
		fmt.Println("Sent signal")
	}()

	consumeMessage(mockConsumer, stopChan)

	mockConsumer.AssertExpectations(t)
	assert.True(t, true)
}
