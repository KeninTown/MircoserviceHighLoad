package config

import (
	"fmt"
	"os"

	"github.com/spf13/viper"
)

type DatabaseConfig struct {
	Host     string `mapstructure:"host"`
	Port     int    `mapstructure:"port"`
	User     string `mapstructure:"user"`
	Password string
	DBname   string `mapstructure:"dbname"`
	SSLMode  string `mapstructure:"sslmode"`
}

type KafkaConfig struct {
	Host  string
	Topic string `mapstructure:"topic"`
}

func Init(path string) (*viper.Viper, error) {
	v := viper.New()
	v.SetConfigFile(path)

	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("failed to read config, err: %s", err.Error())
	}
	return v, nil
}

func ReadDatabaseConfig(v *viper.Viper) (*DatabaseConfig, error) {
	var dbCfg DatabaseConfig
	if err := v.UnmarshalKey("database", &dbCfg); err != nil {
		return nil, fmt.Errorf("failed to read database config")
	}
	dbPassword := os.Getenv("POSTGRES_PASSWORD")
	if dbPassword == "" {
		return nil, fmt.Errorf("failed to read POSTGRES_PASSWORD env variable")
	}

	dbCfg.Password = dbPassword
	return &dbCfg, nil
}

func ReadKafkaConfig(v *viper.Viper) (*KafkaConfig, error) {
	var kCfg KafkaConfig
	if err := v.UnmarshalKey("kafka", &kCfg); err != nil {
		return nil, fmt.Errorf("failed to read database config")
	}

	kafkaHost := os.Getenv("KAFKA_HOST")
	if kafkaHost == "" {
		return nil, fmt.Errorf("failed to read POSTGRES_PASSWORD env variable")
	}

	kCfg.Host = kafkaHost
	return &kCfg, nil
}
