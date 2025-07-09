package constants

const (
	Topic               = "stream-Topic"
	NumPartitions       = 2
	BatchSize           = 2
	KafkaBroker         = "kafka1:19092,kafka2:19093"
	KafkaBrokerInternal = "kafka1:19092,kafka2:19093"
	InputFile           = "/app/input.log"
	ReplicationFactor   = 2
	GroupID             = "stream-consumers"
)
