package config

import (
	"os"
	"runtime"
	"strconv"
)

type Config struct {
	HTTPAddr         string
	KafkaBrokers     []string
	KafkaTopic       string
	KafkaGroupID     string
	ElasticsearchURL string
	NumWorkers       int
}

func LoadConfig() Config {
	cpuCount := runtime.NumCPU()
	cpuCount = 2 // for testing
	numWorkersEnv := os.Getenv("NUM_WORKERS")
	numWorkers := 0
	if numWorkersEnv == "" {
		numWorkers = cpuCount
	} else {
		numWorkersInt, err := strconv.Atoi(numWorkersEnv)
		if err != nil {
			numWorkers = cpuCount
		} else {
			numWorkers = numWorkersInt
		}
	}

	cfg := Config{
		HTTPAddr:         getenv("HTTP_ADDR", ":8080"),
		KafkaBrokers:     []string{getenv("KAFKA_BROKERS", "127.0.0.1:29092")},
		KafkaTopic:       getenv("KAFKA_TOPIC", "portfolio-events"),
		KafkaGroupID:     getenv("KAFKA_GROUP_ID", "api"),
		ElasticsearchURL: getenv("ELASTICSEARCH_URL", "http://localhost:9200"),
		NumWorkers:       numWorkers,
	}

	return cfg
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
