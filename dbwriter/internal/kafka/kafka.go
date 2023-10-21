package kafka

import (
	"context"
	"dbWriter/internal/entities"
	"dbWriter/pkg/sl"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/sagikazarmark/slog-shim"
)

type Repository interface {
	ImportFromCsv(fileName string) error
	FindBiggestId() (int, error)
}

type CsvWriter interface {
	Write(patient entities.Patient, id int) error
	CreateNewFile()
	GetFileName() (string, error)
	GetPathToFile() string
}

type Kafka struct {
	producer sarama.AsyncProducer
	consumer sarama.Consumer
}

func New(addr []string) (*Kafka, error) {
	producer, err := sarama.NewAsyncProducer(addr, sarama.NewConfig())
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka producer, err: %s", err.Error())
	}

	consumer, err := sarama.NewConsumer(addr, sarama.NewConfig())
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka consumer, err: %s", err.Error())
	}

	return &Kafka{
		producer: producer,
		consumer: consumer,
	}, nil
}

func (k Kafka) consumePartition(topic string) (sarama.PartitionConsumer, error) {
	partitionConsumer, err := k.consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		return nil, fmt.Errorf("failed to consume partitio, err: %s", err.Error())
	}
	return partitionConsumer, nil
}

func (k Kafka) Start(ctx context.Context, topic string, cr CsvWriter, r Repository) {
	defer k.consumer.Close()
	defer k.producer.Close()

	var mu sync.Mutex

	//finding biggest id in table
	patientId, err := r.FindBiggestId()
	if err != nil {
		slog.Info("failed to get max(id) from psql", sl.Error(err))
	}

	partitionConsumer, err := k.consumePartition(topic)
	if err != nil {
		slog.Error("failed to consume partition")
		os.Exit(1)
	}
	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			slog.Error("failed to close partitionConsumer", sl.Error(err))
		}
	}()

	go func() {
		for {
			select {
			case <-time.After(time.Second * 15):
				fileName, err := cr.GetFileName()
				if err != nil {
					slog.Error(err.Error())
				}

				mu.Lock()
				if err := r.ImportFromCsv(fileName); err != nil {
					slog.Error("faile to import data from csv file", sl.Error(err))
					continue
				}
				slog.Info("succesfully import data from csv")
				cr.CreateNewFile()
				slog.Info("created file", slog.String("file", fileName))
				mu.Unlock()
			case <-ctx.Done():
				slog.Info("closing importing gorutene")
				return
			}
		}
	}()

	slog.Info("starting to listen kafka")

	for {
		select {
		case msg, ok := <-partitionConsumer.Messages():
			if !ok {
				slog.Error("Kafka's channels closed ")
				break
			}
			var patient entities.Patient
			if err := json.Unmarshal(msg.Value, &patient); err != nil {
				slog.Error("failed to decode msg.Value", sl.Error(err))
			}

			mu.Lock()
			err := cr.Write(patient, patientId)
			if err != nil {
				slog.Error(err.Error())
			}

			patientId++
			mu.Unlock()

			slog.Info("recieved patient", slog.Any("patient", patient))

		//exit app and import data into database before
		//if err does not occures we delete csv file
		case <-ctx.Done():
			if err := r.ImportFromCsv(cr.GetPathToFile()); err != nil {
				slog.Error("faield to import data into csv file before closing")
			} else {
				os.Remove(cr.GetPathToFile())
			}
			slog.Info("db writer is closing")
			return
		}
	}
}
