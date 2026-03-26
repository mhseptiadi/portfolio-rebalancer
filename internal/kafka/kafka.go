package kafka

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"portfolio-rebalancer/internal/models"
	"time"

	"github.com/segmentio/kafka-go"
)

// var writer *kafka.Writer

// MessagePublisher publishes rebalance payloads to the message bus.
type MessagePublisher interface {
	PublishMessage(ctx context.Context, payload []byte) error
}

type Kafka struct {
	topic      string
	writer     *kafka.Writer
	deadWriter *kafka.Writer
	reader     *kafka.Reader
	groupID    string
	brokers    []string
}

var _ MessagePublisher = (*Kafka)(nil)

// InitKafka initializes kafka connection
func InitKafka(kafkaBrokers []string, topic string, kafkaGroupID string) (*Kafka, error) {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(kafkaBrokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{}, // &kafka.LeastBytes{},
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

	deadWriter := &kafka.Writer{
		Addr:                   kafka.TCP(kafkaBrokers...),
		Topic:                  topic + "-dead",
		Balancer:               &kafka.Hash{},
		RequiredAcks:           kafka.RequireOne,
		AllowAutoTopicCreation: true, // for dead letter topic, or create topic manually
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     kafkaBrokers,
		Topic:       topic,
		GroupID:     kafkaGroupID,
		StartOffset: kafka.FirstOffset,
	})

	return &Kafka{
		topic:      topic,
		writer:     writer,
		deadWriter: deadWriter,
		reader:     reader,
		groupID:    kafkaGroupID,
		brokers:    kafkaBrokers,
	}, nil
}

func (k *Kafka) PublishMessage(ctx context.Context, payload []byte) error {
	if k.writer == nil {
		log.Println("Kafka writer is nil; skipping message publish")
		return fmt.Errorf("kafka writer not initialized")
	}

	var req models.UpdatedPortfolio
	err := json.Unmarshal(payload, &req)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	msg := kafka.Message{
		Key:   []byte(req.UserID),
		Value: payload,
		// Headers: []kafka.Header{
		// 	{
		// 		Key:   "user_id",
		// 		Value: []byte(req.UserID),
		// 	},
		// 	{
		// 		Key:   "retry_count",
		// 		Value: []byte("0"),
		// 	},
		// },
	}

	return k.writer.WriteMessages(ctx, msg)
}

func (k *Kafka) commitMessages(msg kafka.Message) error {
	// separate context background to avoid graceful shutdown but message is already processed
	err := k.reader.CommitMessages(context.Background(), msg)
	if err != nil {
		log.Printf("Kafka commit error: %v\n", err)
	}
	log.Printf("Kafka committed message: %v\n", msg.Key)
	return err
}

func (k *Kafka) deadLetterMessage(msg kafka.Message) error {
	ctx := context.Background()

	newMsg := kafka.Message{
		Key:   msg.Key,
		Value: msg.Value,
	}

	// dead letter is important, to not lose the message
	deadLetterRetryCount := 0
	deadLetterDelay := 1 * time.Second
	for {
		err := k.deadWriter.WriteMessages(ctx, newMsg)
		if err == nil {
			return nil
		}
		log.Printf("Kafka dead write error: %v\n", err)
		deadLetterRetryCount++

		time.Sleep(deadLetterDelay)
		deadLetterDelay *= 2
		if deadLetterRetryCount > 5 {
			log.Printf("Kafka dead letter retry count exceeded: %v\n", deadLetterRetryCount)
			return err
		}
	}
}

func (k *Kafka) ConsumeMessage(
	ctx context.Context,
	handler func(context.Context, []byte) error,
) error {
	if k == nil || k.reader == nil {
		return fmt.Errorf("kafka reader not initialized")
	}

	for {
		// msg, err := k.reader.ReadMessage(ctx)
		msg, err := k.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				if errors.Is(ctx.Err(), context.Canceled) {
					return nil
				}
				if errors.Is(ctx.Err(), context.DeadlineExceeded) {
					return nil
				}
				return ctx.Err()
			}

			log.Printf("[kafka] read error: %v", err)
			continue
		}

		if bytes.Equal(msg.Value, []byte("ping")) {
			err = k.commitMessages(msg)
			if err != nil {
				log.Printf("Kafka commit error: %v\n", err)
			}
			continue
		}

		err = func() error {
			retryCount := 0
			delay := 1 * time.Second
			for {
				if err := handler(ctx, msg.Value); err != nil {
					log.Printf("Kafka handler error on retry %d: %v\n", retryCount, err)
					retryCount++
					time.Sleep(delay)
					delay *= 2
					if retryCount > 5 {
						log.Printf("Kafka retry count exceeded: %v\n", retryCount)
						return k.deadLetterMessage(msg)
					}
					continue
				} else {
					return nil
				}
			}
		}()

		if err != nil {
			log.Printf("CRITICAL_DATA_LOSS_PREVENTED | KEY: %s | PAYLOAD: %s | ERR: %v\n",
				string(msg.Key),
				string(msg.Value),
				err,
			)
			// TODO: lock user to avoid borken next process of that particular user (Causal Dependency Violation case)
			continue
		}

		err = k.commitMessages(msg)
		if err != nil {
			log.Printf("Kafka commit error: %v\n", err)
		}
	}
}

func (k *Kafka) Close() []error {
	if k == nil {
		return nil
	}

	var errs []error

	if k.reader != nil {
		if err := k.reader.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if k.writer != nil {
		if err := k.writer.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if k.deadWriter != nil {
		if err := k.deadWriter.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	return errs
}
