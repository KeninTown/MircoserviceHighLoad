package main

import (
	"context"
	"dbWriter/internal/config"
	csvwriter "dbWriter/internal/csvWriter"
	"dbWriter/internal/database"
	"dbWriter/internal/kafka"
	"dbWriter/pkg/sl"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
)

var PatientId = 0

func main() {
	configPath := os.Getenv("CONFIG_PATH")

	cfg, err := config.Init(configPath)
	if err != nil {
		slog.Error("faield to init config", sl.Error(err))
	}

	dbCfg, err := config.ReadDatabaseConfig(cfg)
	if err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}

	kCfg, err := config.ReadKafkaConfig(cfg)
	if err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}

	//created db instanse
	db, err := database.Connect(dbCfg)
	if err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}

	defer db.Close()

	slog.Info("succesfully connect to database")

	//created temp csv file
	csvDirPath, err := filepath.Abs("./temp/")
	if err != nil {
		slog.Info("failed to get abs path to temp dir", sl.Error(err))
		os.Exit(1)
	}

	cw := csvwriter.New(csvDirPath)
	defer cw.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT, os.Interrupt)
	defer stop()

	k, err := kafka.New([]string{kCfg.Host})
	k.Start(ctx, kCfg.Topic, cw, db)
}
