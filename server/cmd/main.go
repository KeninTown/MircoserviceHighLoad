package main

import (
	"HighLoadServer/internal/config"
	"HighLoadServer/internal/server"
	"log"
	"log/slog"
	"os"
)

func main() {
	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		slog.Error("failed to get CONFIG_PATH env variable")
		os.Exit(1)
	}

	cfg, err := config.Init(configPath)
	if err != nil {
		log.Fatal(err.Error())
	}

	serv := server.New(cfg.KafkaHost)

	serv.Run(cfg.Port)
}
