package main

import (
	"context"
	"dbWriter/internal/config"
	csvwriter "dbWriter/internal/csvWriter"
	"dbWriter/internal/database"
	entities "dbWriter/internal/database/models"
	"dbWriter/pkg/sl"
	"encoding/json"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

var PatientId = 0

func main() {
	configPath := os.Getenv("CONFIG_PATH")

	cfg, err := config.Init(configPath)
	if err != nil {
		slog.Error("faield to init config", sl.Error(err))
	}

	//created db instanse
	db, err := database.Connect(cfg)
	if err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
	defer db.Db.Close()

	slog.Info("succesfully connect to database")

	//created temp csv file
	absPathToCsvDir, err := filepath.Abs("./temp/")
	if err != nil {
		slog.Info("failed to get abs path to temp dir", sl.Error(err))
		os.Exit(1)
	}

	//create CsvWriter with *file
	csvWriter := csvwriter.New()
	csvWriter.CreateNewFile(absPathToCsvDir)
	if err != nil {
		slog.Error("failed to create temp file", sl.Error(err))
		os.Exit(1)
	}
	slog.Info("created file", slog.String("file", csvWriter.File.Name()))

	//close file after complete
	defer csvWriter.File.Close()

	//find the biggest id in patients table
	PatientId, err := db.FindBiggestId()
	if err != nil {
		slog.Error(err.Error())
	}

	log.Println("KafkaHost", cfg.KafkaHost)
	consumer, err := sarama.NewConsumer([]string{cfg.KafkaHost}, sarama.NewConfig())
	if err != nil {
		slog.Error("failed  to init consumer", slog.String("kafkaHost", cfg.KafkaHost), sl.Error(err))
		os.Exit(1)
	}
	log.Println("consumer, ", consumer)
	defer consumer.Close()

	partitionConsumer, err := consumer.ConsumePartition("test", 0, sarama.OffsetNewest)
	if err != nil {
		slog.Error("failed to init partition consumer", sl.Error(err))
		os.Exit(1)
	}
	defer partitionConsumer.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT, os.Interrupt)
	defer stop()

	//mutex for sync writing file and PatientId
	var mu sync.Mutex

	//looking for file's size
	//when it reached some big value we import
	//csv into PostgreSQL, delete old file and create a new one
	go func() {
		for {
			select {
			case <-time.After(time.Second * 15):
				fileInfo, err := csvWriter.File.Stat()
				if err != nil {
					log.Println("failed to get file info: %w", err)
					continue
				}
				if fileInfo.Size() > 1 {

					mu.Lock()

					if err := db.ImportFromCsv(fileInfo.Name()); err != nil {
						slog.Error("faile to import data from csv file", sl.Error(err))
						continue
					}

					slog.Info("succesfully import data from csv")
					csvWriter.CreateNewFile(absPathToCsvDir)
					slog.Info("created file", slog.String("file", csvWriter.File.Name()))
					mu.Unlock()
				}
			case <-ctx.Done():
				slog.Info("closing importing gorutene")
				return
			}
		}
	}()

	slog.Info("start to listen kafka")

	//listen kafka channel and write incoming
	//data into csv file
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
			csvWriter.Write(patient, PatientId)
			PatientId++
			mu.Unlock()
			slog.Info("recieved patient", slog.Any("patient", patient))

		//exit app and import data into database before
		//if err does not occures we delete csv file
		case <-ctx.Done():
			if err := db.ImportFromCsv(csvWriter.File.Name()); err != nil {
				slog.Error("faield to import data into csv file before closing")
			} else {
				os.Remove(csvWriter.File.Name())
			}
			slog.Info("db writer is closing")
			return
		}
	}
}
