package rabbitmq

import (
	"sync"
	"time"

	"app/src/infrastructure/config"

	e "github.com/ChatDetectiveORG/shared/errors"

	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	clientOnce sync.Once
	client     *Client
)

// GetClient returns a singleton RabbitMQ client.
//
// Env:
// - RABBITMQ_URL (preferred), e.g. amqp://guest:guest@localhost:5672/
func GetClient(config *config.Config) (*Client, *e.ErrorInfo) {
	var initErr *e.ErrorInfo = e.Nil()

	clientOnce.Do(func() {
		url := config.RabbitMQConfig.URL
		if url == "" {
			initErr = e.NewError("missing env RABBITMQ_URL", "rabbitmq url is not configured").WithSeverity(e.Critical)
			return
		}

		// Heartbeat: интервал отправки heartbeat пакетов между клиентом и сервером RabbitMQ, помогает обнаруживать разрывы соединения (здесь 10 секунд).
		// Locale: локализация для работы с сервером (по умолчанию "en_US").
		// Dial: функция установки TCP-соединения с таймаутом (здесь 10 секунд), защищает от "вечного" ожидания при сетевых проблемах.
		cfg := amqp.Config{
			Heartbeat: 10 * time.Second,                   // Период heartbeat сообщений между приложением и RabbitMQ
			Locale:    "en_US",                            // Язык сервера/клиента
			Dial:      amqp.DefaultDial(10 * time.Second), // Таймаут создания TCP-соединения с сервером
		}

		c, err := NewClient(url, cfg)
		if err != nil {
			initErr = e.FromError(err, "failed to connect to rabbitmq").WithSeverity(e.Critical)
			return
		}
		client = c
	})

	if !initErr.IsNil() {
		return nil, initErr
	}
	return client, e.Nil()
}

// InitRabbitMQ connects to RabbitMQ and ensures required topology models.
// "Verification" here is done by idempotent declarations; if a declaration mismatches existing settings,
// RabbitMQ will return an error (channel exception), which we surface.
func InitRabbitMQ(config *config.Config, models []Model) *e.ErrorInfo {
	c, err := GetClient(config)
	if !err.IsNil() {
		return err
	}

	ch, chErr := c.Channel()
	if chErr != nil {
		return e.FromError(chErr, "failed to open rabbitmq channel").WithSeverity(e.Critical)
	}
	defer func() { _ = ch.Close() }()

	if err := EnsureModels(ch, models); err != nil {
		return e.FromError(err, "failed to ensure rabbitmq models").WithSeverity(e.Critical)
	}

	return e.Nil()
}

func NewRabbitmqChannel(cfg *config.Config) (*amqp.Channel, *e.ErrorInfo) {
	client, err := GetClient(cfg)
	if !err.IsNil() {
		return nil, err
	}

	ch, unwrappedError := client.Channel()
	if unwrappedError != nil {
		return nil, e.FromError(unwrappedError, "failed to get rabbitmq channel")
	}

	return ch, e.Nil()
}
