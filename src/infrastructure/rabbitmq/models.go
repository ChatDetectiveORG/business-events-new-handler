package rabbitmq

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Model is a declarative piece of RabbitMQ topology (exchange/queue/binding, etc).
// Ensure should be idempotent: calling it multiple times should not break.
//
// Important: if the model declaration mismatches an already existing entity,
// RabbitMQ will return an error (channel exception), which effectively "verifies" your topology.
type Model interface {
	Name() string
	Ensure(ch *amqp.Channel) error
}

func EnsureModels(ch *amqp.Channel, models []Model) error {
	for _, m := range models {
		if m == nil {
			continue
		}
		if err := m.Ensure(ch); err != nil {
			return fmt.Errorf("%s: %w", m.Name(), err)
		}
	}
	return nil
}

// ExchangeModel declares an exchange.
// Template: customize fields as you need.
type ExchangeModel struct {
	Exchange   string
	Kind       string // "direct", "topic", "fanout", "headers"
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Args       amqp.Table
}

func (m ExchangeModel) Name() string { return "exchange:" + m.Exchange }

func (m ExchangeModel) Ensure(ch *amqp.Channel) error {
	return ch.ExchangeDeclare(
		m.Exchange,
		m.Kind,
		m.Durable,
		m.AutoDelete,
		m.Internal,
		m.NoWait,
		m.Args,
	)
}

// QueueModel declares a queue.
// Template: customize fields as you need.
type QueueModel struct {
	Queue      string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       amqp.Table
}

func (m QueueModel) Name() string { return "queue:" + m.Queue }

func (m QueueModel) Ensure(ch *amqp.Channel) error {
	_, err := ch.QueueDeclare(
		m.Queue,
		m.Durable,
		m.AutoDelete,
		m.Exclusive,
		m.NoWait,
		m.Args,
	)
	return err
}

// BindingModel binds a queue to an exchange.
// Template: customize fields as you need.
type BindingModel struct {
	Queue      string
	Exchange   string
	RoutingKey string
	NoWait     bool
	Args       amqp.Table
}

func (m BindingModel) Name() string {
	return fmt.Sprintf("binding:%s<=%s[%s]", m.Queue, m.Exchange, m.RoutingKey)
}

func (m BindingModel) Ensure(ch *amqp.Channel) error {
	return ch.QueueBind(
		m.Queue,
		m.RoutingKey,
		m.Exchange,
		m.NoWait,
		m.Args,
	)
}
