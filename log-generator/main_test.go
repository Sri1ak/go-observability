package main

import (
	"os"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type MockKafkaProducer struct {
	mock.Mock
}

func (m *MockKafkaProducer) Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error {
	args := m.Called(msg, deliveryChan)
	return args.Error(0)
}

func (m *MockKafkaProducer) Flush(timeoutMs int) int {
	args := m.Called(timeoutMs)
	return args.Int(0)
}

func (m *MockKafkaProducer) Close() {
	m.Called()
}
func Test_SendBatch(t *testing.T) {

	mockProducer := new(MockKafkaProducer)
	topic := "test-topic"
	batch := []string{
		"service1:message1",
		"service2:message2",
	}

	// Set expectations for valid messages
	mockProducer.On("Produce", mock.MatchedBy(func(msg *kafka.Message) bool {
		return string(msg.Key) == "service1" && string(msg.Value) == "message1"
	}), mock.Anything).Return(nil)

	mockProducer.On("Produce", mock.MatchedBy(func(msg *kafka.Message) bool {
		return string(msg.Key) == "service2" && string(msg.Value) == "message2"
	}), mock.Anything).Return(nil)

	sendBatch(batch, topic, mockProducer)

	mockProducer.AssertNumberOfCalls(t, "Produce", 2)
	mockProducer.AssertExpectations(t)
}

func TestProduceMessagesFromFile(t *testing.T) {
	// Create a temp input file
	content := "svc1:msg1\nsvc2:msg2\nsvc3:msg3\n"
	tmpFile, err := os.CreateTemp("", "input*.txt")
	assert.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	_, _ = tmpFile.WriteString(content)
	tmpFile.Close()

	mockProducer := new(MockKafkaProducer)
	mockProducer.On("Flush", 5000).Return(0)
	mockProducer.On("Produce", mock.Anything, mock.Anything).Return(nil)

	produceMessagesFromFile(mockProducer, tmpFile.Name())

	mockProducer.AssertNumberOfCalls(t, "Produce", 3)
}
