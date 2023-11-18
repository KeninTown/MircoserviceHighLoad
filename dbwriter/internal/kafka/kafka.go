package kafka

import (
	"context"
	"dbWriter/internal/common/commonerr"
	"dbWriter/internal/entities"
	"dbWriter/pkg/sl"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"golang.org/x/exp/slog"
)

type Repository interface {
	ImportFromCsv(fileName string) error
	FindBiggestId() (int, error)
	FindPatient(id int) (entities.Patient, error)
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
		return nil, fmt.Errorf("failed to consume partition, err: %s", err.Error())
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

	//consume createPatient partitions
	createPatientConsumer, err := k.consumePartition(topic)
	if err != nil {
		slog.Error("failed to consume partition")
		os.Exit(1)
	}
	defer func() {
		if err := createPatientConsumer.Close(); err != nil {
			slog.Error("failed to close partitionConsumer", sl.Error(err))
		}
	}()

	//consume patientId partition
	patientIdConsumer, err := k.consumePartition("patientId")
	if err != nil {
		slog.Error("failed to consume patientId partition")
		os.Exit(1)
	}
	defer func() {
		if err := patientIdConsumer.Close(); err != nil {
			slog.Error("failed to close patientId consumer", sl.Error(err))
		}
	}()

	//importing data from csv file
	go func() {
	loop:
		for {
			select {
			case <-time.After(time.Minute):
				fileName, err := cr.GetFileName()
				if err != nil {
					slog.Error(err.Error())
				}

				mu.Lock()
				if err := r.ImportFromCsv(fileName); err != nil {
					slog.Error("faile to import data from csv file", sl.Error(err))
					continue loop
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

infinityLoop:
	for {
		select {
		//create new patient
		case msg, ok := <-createPatientConsumer.Messages():
			if !ok {
				slog.Error("Kafka's channels closed ")
				break infinityLoop
			}

			var patient entities.Patient

			if err := json.Unmarshal(msg.Value, &patient); err != nil {
				slog.Error("failed to decode msg.Value", sl.Error(err))
				k.sendError(msg.Key, "failed to unmarshal")
				continue infinityLoop
			}

			slog.Info("recieved patient", slog.Any("patient", patient))

			mu.Lock()

			err := cr.Write(patient, patientId)
			if err != nil {
				slog.Error(err.Error())
				k.sendError(msg.Key, err.Error())
				continue infinityLoop
			}
			patient.Id = uint(patientId)
			patientId++

			mu.Unlock()

			patientData, err := json.Marshal(patient)
			if err != nil {
				slog.Error("failed to unmarshal", slog.String("msgId", string(msg.Key)))
				k.sendError(msg.Key, "failed to unmarshal")
				continue infinityLoop
			}

			k.sendMsg(msg.Key, patientData)
			fmt.Println("patient send with id = ", patient.Id, " chanId = ", string(msg.Key))

		//recieve patient's id and send patient's data
		case msg, ok := <-patientIdConsumer.Messages():
			if !ok {
				slog.Error("Kafka's channels closed ")
				break infinityLoop
			}

			idStr := string(msg.Value)
			id, err := strconv.Atoi(idStr)

			if err != nil || id <= 0 {
				slog.Error("invalid id")
				k.sendError(msg.Key, "invalid id")
				continue infinityLoop
			}

			fmt.Println("id = ", id)
			patient, err := r.FindPatient(int(id))
			fmt.Println("patient = ", patient)

			if err != nil {
				slog.Error(err.Error())
				k.sendError(msg.Key, err.Error())
				continue infinityLoop
			}

			patientData, err := json.Marshal(patient)
			if err != nil {
				slog.Error(err.Error())
				k.sendError(msg.Key, err.Error())
				continue infinityLoop
			}

			k.sendMsg(msg.Key, patientData)

			fmt.Println("patient sent")

		//exit app and import data into database before exit
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

func (k Kafka) sendMsg(key, value []byte) {
	patientInfoMsg := &sarama.ProducerMessage{
		Topic: "patientInfo",
		Key:   sarama.ByteEncoder(key),
		Value: sarama.ByteEncoder(value),
	}

	k.producer.Input() <- patientInfoMsg
}

func (k Kafka) sendError(key []byte, msg string) {
	err := commonerr.New(msg)
	errData, _ := json.Marshal(err)

	k.sendMsg(key, errData)
}
