package rabbitmq

import (
	"app/src/infrastructure/config"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// RequiredModels is a starter template for RabbitMQ topology.
// Customize it for your app (names, durability, DLX, routing keys, etc).
//
// Tip: model declarations are idempotent, so calling InitRabbitMQ on every boot is fine.
var RequiredModels = buildRequiredModels()

const shardCount = 64

func buildRequiredModels() []Model {
	models := []Model{
		ExchangeModel{
			Exchange:   "chatdetective.events",
			Kind:       "topic",
			Durable:    true,
			AutoDelete: false,
			Internal:   false,
			NoWait:     false,
			Args:       amqp.Table{},
		},
		ExchangeModel{
			Exchange:   "chatdetective.output.send",
			Kind:       "direct",
			Durable:    true,
			AutoDelete: false,
			Internal:   false,
			NoWait:     false,
			Args:       amqp.Table{},
		},
	}

	for i := 0; i < shardCount; i++ {
		q := fmt.Sprintf("%s.q%02d", config.PodType, i)
		models = append(models,
			QueueModel{
				Queue:      q,
				Durable:    true,
				AutoDelete: false,
				Exclusive:  false,
				NoWait:     false,
				Args: amqp.Table{
					// Multiple pods may subscribe to same queue, but only one is active.
					// This avoids podId-based routing while keeping per-queue ordering.
					"x-single-active-consumer": true,
				},
			},
			BindingModel{
				Queue:      q,
				Exchange:   "chatdetective.events",
				RoutingKey: q,
				NoWait:     false,
				Args:       amqp.Table{},
			},
		)
	}

	return models
}
