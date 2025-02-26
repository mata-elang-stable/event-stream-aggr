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
	failedCount int
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
	total           int64
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
	failedEventsTotal := 0
	latencyTotal := time.Duration(0)

	for _, task := range completedTasks {
		if task.isFailed {
			failedBatchTotal++
			continue
		}
		failedEventsTotal += task.failedCount
		eventProcessedTotal += task.TotalEvents
		latencyTotal += task.Duration
	}

	a.total += eventProcessedTotal

	log.Infof("[%d] Sent %d events (avg %.2f/s) workers %d, waiting %d, failed %d. avg latency %.2f ms\n",
		a.total,
		eventProcessedTotal,
		float64(eventProcessedTotal)/float64(a.logInterval),
		batchEventProcessedTotal,
		waitingTasks,
		failedEventsTotal,
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

					count, failedCount, err := a.processMessage(e)
					if err != nil {
						log.Errorf("Failed to process message: %v\n", err)
						//a.currentFailedBatch.Add(1)
						isFailed = true
					}

					if failedCount > 0 {
						log.Errorf("Failed to process message: %v\n", failedCount)
					}

					taskStats := &CompletedEvent{
						TotalEvents: count,
						Duration:    time.Since(startTime),
						isFailed:    isFailed,
						failedCount: failedCount,
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

func (a *App) processMessage(msg *kafka.Message) (int64, int, error) {
	startTime := time.Now()

	value, err := a.deserializer.Deserialize(a.inputTopic, msg.Value)
	if err != nil {
		return 0, 0, err
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
	failedEventCount := 0

	for _, metric := range payload.Metrics {
		payloadJson := processor.GetRawDataFromMetrics(payload, metric)

		eventTime, err := time.Parse("06/01/02-15:04:05.999999", payloadJson.Timestamp)
		if err != nil {
			eventTime = time.Unix(payloadJson.Seconds, 0)
		}

		payloadByte, err := a.serializer.Serialize(a.outputTopic, payloadJson)
		if err != nil {
			failedEventCount++
			continue
		}

		if err := a.ProduceMessage(&kafka.Message{
			TopicPartition: topicPartition,
			Key:            eventHashSha256Byte,
			Value:          payloadByte,
			Headers:        headerMessage,
			Timestamp:      eventTime,
		}); err != nil {
			failedEventCount++
			continue
		}
	}

	log.Tracef("[%s] finished sending %d events in %s\n", payload.EventHashSha256[:8], payload.EventMetricsCount, time.Since(startTime).String())

	if _, err := a.consumer.CommitMessage(msg); err != nil {
		return payload.EventMetricsCount, failedEventCount, err
	}

	return payload.EventMetricsCount, failedEventCount, nil
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
