package metrics

import (
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

/*
*
Producer and Consumer uses the shared metrics to send the data to prometheus for visualization
ProducedMessages - producer sends the data of the messages produced
LogsConsumedTotal - Total number of logs per service consumed by the consumer
*
*/
var (
	ProducedMessages = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "kafka_produced_messages_total",
		Help: "Total messages produced to Kafka",
	},
	)
	LogsConsumedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "logs_consumed_total",
			Help: "Total logs consumed from Kafka",
		},
		[]string{"service"},
	)
)

/*
*
Init method to send the metrics for the producer and consumer
to the prometheus
*
*/
func Init(portInit string) {
	prometheus.MustRegister(ProducedMessages)
	prometheus.MustRegister(LogsConsumedTotal)

	port := os.Getenv("METRICS_PORT")
	fmt.Println("invoked port", port)
	if port == "" {
		port = portInit
	}

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Printf("Starting metrics server on :%s/metrics\n", port)
		err := http.ListenAndServe(":"+port, nil)
		if err != nil {
			log.Fatalf("Metrics HTTP server error: %v", err)
		}
	}()

}
