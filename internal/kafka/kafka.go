package kafka

import (
	"bytes"
	"context"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

var writer *kafka.Writer

type Kafka struct {
	topic   string
	writer  *kafka.Writer
	reader  *kafka.Reader
	groupID string
	brokers []string
}

// InitKafka initializes kafka connection
func InitKafka(kafkaBrokers []string, topic string, kafkaGroupID string) (*Kafka, error) {
	log.Println("InitKafka==", kafkaBrokers, topic, kafkaGroupID)
	writer = &kafka.Writer{
		Addr:         kafka.TCP(kafkaBrokers...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireOne,
	}

	// Retry logic to check Kafka availability
	// for i := 0; i < 10; i++ {
	// 	err := writer.WriteMessages(context.Background(), kafka.Message{
	// 		Value: []byte("ping"),
	// 	})
	// 	if err == nil {
	// 		log.Println("Kafka is ready")
	// 		break
	// 	}
	// 	log.Println("Waiting for Kafka to be ready...")
	// 	time.Sleep(2 * time.Second)
	// }

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     kafkaBrokers,
		Topic:       topic,
		GroupID:     kafkaGroupID,
		StartOffset: kafka.FirstOffset,
	})

	return &Kafka{
		topic:   topic,
		writer:  writer,
		reader:  reader,
		groupID: kafkaGroupID,
		brokers: kafkaBrokers,
	}, nil
}

func (k *Kafka) PublishMessage(ctx context.Context, payload []byte) error {
	if k.writer == nil {
		log.Println("Kafka writer is nil; skipping message publish")
		return fmt.Errorf("kafka writer not initialized")
	}

	msg := kafka.Message{
		Value: payload,
	}

	return k.writer.WriteMessages(ctx, msg)
}

// func (k *Kafka) ConsumeMessage(ctx context.Context, handler func([]byte) error) error {
// 	go func() {
// 		defer k.reader.Close()
// 		for {
// 			msg, err := k.reader.ReadMessage(ctx)
// 			if err != nil {
// 				log.Printf("Kafka read error: %v\n", err)
// 				continue
// 			}

// 			if bytes.Equal(msg.Value, []byte("ping")) {
// 				continue
// 			}

// 			if err := handler(msg.Value); err != nil {
// 				log.Printf("Kafka handler error: %v\n", err)
// 				continue
// 			}
// 		}
// 	}()

// 	log.Println("Kafka consumer started")
// 	return nil
// }

func (k *Kafka) ConsumeMessage(
	ctx context.Context,
	handler func([]byte) error,
) error {
	if k == nil || k.reader == nil {
		return fmt.Errorf("kafka reader not initialized")
	}

	for {
		msg, err := k.reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			// For skeleton: keep going on transient errors.
			log.Printf("[kafka] read error: %v", err)
			continue
		}

		if bytes.Equal(msg.Value, []byte("ping")) {
			continue
		}

		if err := handler(msg.Value); err != nil {
			log.Printf("Kafka handler error: %v\n", err)
			continue
		}
	}
}
