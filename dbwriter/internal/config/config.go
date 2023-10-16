package config

import (
	"fmt"
	"os"

	"github.com/spf13/viper"
)

type Config struct {
	KafkaHost string
	Host      string `mapstructure:"host"`
	Port      int    `mapstructure:"port"`
	Password  string
	User      string `mapstructure:"user"`
	DBname    string `mapstructure:"dbname"`
	SSLMode   string `mapstructure:"sslmode"`
}

func Init(path string) (*Config, error) {
	var cfg Config

	v := viper.New()
	v.SetConfigFile(path)

	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("failed to read config, err: %s", err.Error())
	}

	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config, err: %s", err.Error())
	}

	dbPassword := os.Getenv("POSTGRES_PASSWORD")
	if dbPassword == "" {
		return nil, fmt.Errorf("failed to read POSTGRES_PASSWORD env variable")
	}

	kafkaHost := os.Getenv("KAFKA_HOST")
	if kafkaHost == "" {
		return nil, fmt.Errorf("failed to read KAFKA_HOST env variable")
	}

	cfg.Password = dbPassword
	cfg.KafkaHost = kafkaHost

	return &cfg, nil
}
