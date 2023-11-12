package main

import (
	"HighLoadServer/internal/config"
	"HighLoadServer/internal/server"
	"log/slog"
	"os"
)

// TODO: сделать root /:patientId для получения информации о пациенте + kafka +
// + отдельный сервис для выдачи информации о пациента
func main() {
	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		slog.Error("failed to get CONFIG_PATH env variable")
		os.Exit(1)
	}

	cfg, err := config.Init(configPath)
	if err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}

	serv, err := server.New(cfg.KafkaHost)
	if err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}

	serv.Run(cfg.Port)
}
