package config

import (
	"fmt"
	"os"

	"github.com/spf13/viper"
)

type Config struct {
	Port      string `mapstructure:"port"`
	KafkaHost string
}

func Init(path string) (*Config, error) {
	var cfg Config
	v := viper.New()
	v.SetConfigFile(path)
	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("failed to read config file, path = %s, err = %s", path, err.Error())
	}

	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal cfg")
	}

	cfg.KafkaHost = os.Getenv("KAFKA_HOST")
	if cfg.KafkaHost == "" {
		return nil, fmt.Errorf("failed to read KAFKA_HOST env variable")
	}

	return &cfg, nil
}
