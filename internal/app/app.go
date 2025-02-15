package app

import (
	"context"
	"sync"
	"time"

	"github.com/alitto/pond/v2"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avro"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/protobuf"
	"github.com/mata-elang-stable/event-stream-aggr/internal/logger"
	"github.com/mata-elang-stable/event-stream-aggr/internal/pb"
	"github.com/mata-elang-stable/event-stream-aggr/internal/processor"
)

var log = logger.GetLogger()

type CompletedEvent struct {
	TotalEvents int64
	Duration    time.Duration
	isFailed    bool
}

type App struct {
	consumer        *kafka.Consumer
	producer        *kafka.Producer
	deserializer    *protobuf.Deserializer
	serializer      *avro.GenericSerializer
	inputTopic      string
	outputTopic     string
	maxConcurrency  int
	pool            pond.Pool
	logInterval     int
	tasksStats      []*CompletedEvent
	tasksStatsMutex sync.Mutex
}

func NewApp(
	consumer *kafka.Consumer,
	deserializer *protobuf.Deserializer,
	producer *kafka.Producer,
	serializer *avro.GenericSerializer,
	inputTopic string,
	outputTopic string,
	maxConcurrency int,
	logInterval int,
) *App {
	return &App{
		consumer:       consumer,
		deserializer:   deserializer,
		producer:       producer,
		serializer:     serializer,
		inputTopic:     inputTopic,
		outputTopic:    outputTopic,
		maxConcurrency: maxConcurrency,
		pool:           pond.NewPool(maxConcurrency),
		logInterval:    logInterval,
		tasksStats:     make([]*CompletedEvent, 0),
	}
}

// GetAndResetProcessedData returns the processed data and resets the internal state
func (a *App) GetAndResetProcessedData() []*CompletedEvent {
	a.tasksStatsMutex.Lock()
	defer a.tasksStatsMutex.Unlock()

	data := a.tasksStats
	a.tasksStats = make([]*CompletedEvent, 0)
	return data
}

func (a *App) PrintStats() {
	waitingTasks := a.pool.WaitingTasks()
	completedTasks := a.GetAndResetProcessedData()
	batchEventProcessedTotal := len(completedTasks)

	if batchEventProcessedTotal < 1 {
		return
	}

	eventProcessedTotal := int64(0)
	failedBatchTotal := int64(0)
	latencyTotal := time.Duration(0)

	for _, task := range completedTasks {
		if task.isFailed {
			failedBatchTotal++
			continue
		}
		eventProcessedTotal += task.TotalEvents
		latencyTotal += task.Duration
	}

	log.Infof("Sent %d events (avg %.2f/s) workers %d, waiting %d, failed %d tasks. avg latency %.2f ms\n",
		eventProcessedTotal,
		float64(eventProcessedTotal)/float64(a.logInterval),
		batchEventProcessedTotal,
		waitingTasks,
		failedBatchTotal,
		float64(latencyTotal.Milliseconds())/float64(batchEventProcessedTotal),
	)
}

func (a *App) Run(ctx context.Context) error {
	log.Infof("Starting to consume messages from topic: %s\n", a.inputTopic)

	ticker := time.NewTicker(time.Duration(a.logInterval) * time.Second)

EventLoop:
	for {
		select {
		case <-ctx.Done():
			break EventLoop
		case <-ticker.C:
			a.PrintStats()
		default:
			ev := a.consumer.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				a.pool.Submit(func() {
					startTime := time.Now()
					isFailed := false

					count, err := a.processMessage(e)
					if err != nil {
						log.Errorf("Failed to process message: %v\n", err)
						//a.currentFailedBatch.Add(1)
						isFailed = true
					}

					taskStats := &CompletedEvent{
						TotalEvents: count,
						Duration:    time.Since(startTime),
						isFailed:    isFailed,
					}

					a.tasksStatsMutex.Lock()
					defer a.tasksStatsMutex.Unlock()
					a.tasksStats = append(a.tasksStats, taskStats)
				})
			case kafka.Error:
				log.Fatalf("Kafka error: %v\n", e)
			default:
				continue
			}

		}
	}

	log.Infof("Starting to shutdown the app...\n")
	a.shutdown()

	return nil
}

func (a *App) processMessage(msg *kafka.Message) (int64, error) {
	startTime := time.Now()

	value, err := a.deserializer.Deserialize(a.inputTopic, msg.Value)
	if err != nil {
		return 0, err
	}

	payload := value.(*pb.SensorEvent)
	eventHashSha256Byte := []byte(payload.EventHashSha256)
	headerMessage := []kafka.Header{
		{Key: "hash_sha256", Value: []byte(payload.EventHashSha256)},
		{Key: "sensor_id", Value: []byte(payload.SensorId)},
		{Key: "priorityStr", Value: []byte(processor.ParsePriority(payload.SnortPriority))},
		{Key: "classification", Value: []byte(*payload.SnortClassification)},
	}
	topicPartition := kafka.TopicPartition{Topic: &a.outputTopic, Partition: kafka.PartitionAny}

	draftResult := make([]*kafka.Message, 0)

	for _, metric := range payload.Metrics {
		payloadJson := processor.GetRawDataFromMetrics(payload, metric)

		eventTime, err := time.Parse("06/01/02-15:04:05.999999", payloadJson.Timestamp)
		if err != nil {
			eventTime = time.Unix(payloadJson.Seconds, 0)
		}

		payloadByte, err := a.serializer.Serialize(a.outputTopic, payloadJson)
		if err != nil {
			return 0, err
		}

		draftResult = append(draftResult, &kafka.Message{
			TopicPartition: topicPartition,
			Key:            eventHashSha256Byte,
			Value:          payloadByte,
			Headers:        headerMessage,
			Timestamp:      eventTime,
		})
	}
	log.Tracef("[%s] finished processing %d events in %s\n", payload.EventHashSha256[:8], payload.EventMetricsCount, time.Since(startTime).String())

	// Check if all messages are processed, with total of payload.EventMetricsCount
	if len(draftResult) != int(payload.EventMetricsCount) {
		return 0, err
	}

	// sent to result channel
	for _, result := range draftResult {
		log.Tracef("[draft] Producing message: %s\n", result.TopicPartition)

		if err := a.ProduceMessage(result); err != nil {
			return 0, err
		}
	}
	log.Tracef("[%s] finished sending %d events in %s\n", payload.EventHashSha256[:8], payload.EventMetricsCount, time.Since(startTime).String())

	if _, err := a.consumer.CommitMessage(msg); err != nil {
		return 0, err
	}

	return payload.EventMetricsCount, nil
}

func (a *App) ProduceMessage(msg *kafka.Message) error {
	deliveryChan := make(chan kafka.Event)

	if err := a.producer.Produce(msg, deliveryChan); err != nil {
		return err
	}

	deliveryReport := <-deliveryChan
	switch r := deliveryReport.(type) {
	case *kafka.Message:
		if r.TopicPartition.Error != nil {
			log.Errorf("Delivery failed: %v\n", r.TopicPartition.Error)
			return r.TopicPartition.Error
		}

		log.Tracef("Delivered message to %v\n", r.TopicPartition)
	}

	return nil
}

func (a *App) shutdown() {
	log.Infof("Stopping worker pool...\n")
	a.pool.StopAndWait()

	log.Infof("Flushing producer...\n")
	unFlushedCount := a.producer.Flush(5 * 60 * 1000) // 5 minutes
	if unFlushedCount > 0 {
		log.Errorf("Timeout flushing producer, %d events remain unflushed\n", unFlushedCount)
	}

	log.Infof("Closing producer...\n")
	a.producer.Close()

	log.Infof("Closing consumer...\n")
	if err := a.consumer.Close(); err != nil {
		log.Errorf("Error closing consumer: %v\n", err)
	}
}
