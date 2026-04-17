package metrics

import (
	"github.com/ChatDetectiveORG/business-events-new-handler/src/infrastructure/config"
	"github.com/ChatDetectiveORG/business-events-new-handler/src/infrastructure/rabbitmq"
	"context"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const shardCount = 64

func startRabbitMQQueueCollector(ctx context.Context, cfg *config.Config) {
	ch, err := rabbitmq.NewRabbitmqChannel(cfg)
	if !err.IsNil() {
		log.Printf("metrics collector: failed to open rabbitmq channel: %s", err.JSON())
		return
	}

	go func() {
		<-ctx.Done()
		_ = ch.Close()
	}()

	interval := 5 * time.Second
	if v := cfg.RuntimeConfig; v != nil && v.NumRoutingGorutines > 0 {
		// No dedicated config yet; keep fixed interval.
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	collect := func() {
		for i := 0; i < shardCount; i++ {
			q := fmt.Sprintf("%s.q%02d", config.PodType, i)
			info, err := passiveQueueInfo(ch, q)
			if err != nil {
				queueMessages.WithLabelValues(q).Set(-1)
				queueConsumers.WithLabelValues(q).Set(-1)
				log.Printf("metrics collector: queue=%s error=%v", q, err)
				continue
			}
			queueMessages.WithLabelValues(q).Set(float64(info.Messages))
			queueConsumers.WithLabelValues(q).Set(float64(info.Consumers))
		}
	}

	collect()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			collect()
		}
	}
}

type queueInfo struct {
	Messages  int
	Consumers int
}

func passiveQueueInfo(ch *amqp.Channel, queue string) (queueInfo, error) {
	q, err := ch.QueueDeclarePassive(
		queue,
		true,  // durable
		false, // autoDelete
		false, // exclusive
		false, // noWait
		amqp.Table{},
	)
	if err != nil {
		return queueInfo{}, fmt.Errorf("queue declare passive failed: %w", err)
	}
	return queueInfo{
		Messages:  q.Messages,
		Consumers: q.Consumers,
	}, nil
}
