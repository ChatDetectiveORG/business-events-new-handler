package config

import (
	e "app/pkg/errors"
	"time"

	"github.com/spf13/viper"
)

const PodType = "business_events_new"

type Config struct {
	RuntimeConfig  *RuntimeConfig
	RabbitMQConfig *RabbitMQConfig
	PostgresConfig *PostgresConfig
	RedisConfig    *RedisConfig
}

type RuntimeConfig struct {
	NumRoutingGorutines int
	PodID               string
	PodType             string
	HandlerLiveDuration time.Duration
}

type RabbitMQConfig struct {
	URL string
}

type PostgresConfig struct {
	Host     string
	Port     string
	User     string
	Password string
	Database string
}

type RedisConfig struct {
	Host     string
	Port     string
	Password string
	Database int

	MaxIdle     int
	MaxActive   int
	IdleTimeout time.Duration
	Wait        bool

	ConnectionTimeout time.Duration
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
}

// Fetches config from environment variables
func FetchConfig() (*Config, *e.ErrorInfo) {
	viper.AutomaticEnv()

	config := &Config{
		PostgresConfig: &PostgresConfig{
			Host:     viper.GetString("POSTGRES_HOST"),
			Port:     viper.GetString("POSTGRES_PORT"),
			User:     viper.GetString("POSTGRES_USER"),
			Password: viper.GetString("POSTGRES_PASSWORD"),
			Database: viper.GetString("POSTGRES_DB"),
		},
		RedisConfig: &RedisConfig{
			Host:              viper.GetString("REDIS_HOST"),
			Port:              viper.GetString("REDIS_PORT"),
			Password:          viper.GetString("REDIS_PASSWORD"),
			Database:          viper.GetInt("REDIS_DB"),
			MaxIdle:           viper.GetInt("REDIS_MAX_IDLE"),
			MaxActive:         viper.GetInt("REDIS_MAX_ACTIVE"),
			IdleTimeout:       viper.GetDuration("REDIS_IDLE_TIMEOUT"),
			Wait:              viper.GetBool("REDIS_WAIT"),
			ConnectionTimeout: viper.GetDuration("REDIS_CONNECTION_TIMEOUT"),
			ReadTimeout:       viper.GetDuration("REDIS_READ_TIMEOUT"),
			WriteTimeout:      viper.GetDuration("REDIS_WRITE_TIMEOUT"),
		},
		RabbitMQConfig: &RabbitMQConfig{
			URL: viper.GetString("RABBITMQ_URL"),
		},
		RuntimeConfig: &RuntimeConfig{
			NumRoutingGorutines: viper.GetInt("NUM_ROUTING_GOROUTINES"),
			PodID:               viper.GetString("POD_ID"),
			PodType:             PodType,
			HandlerLiveDuration: time.Minute * 10,
		},
	}

	return config, e.Nil()
}
