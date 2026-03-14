package application

import (
	e "app/pkg/errors"
	"app/src/infrastructure/config"
	"app/src/infrastructure/rabbitmq"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	tele "gopkg.in/telebot.v4"

	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	gorutineEndpoints = make(map[string]chan amqp.Delivery, 1000)
	handlerTerminated = make(chan string, 1000)
	errors            = make(chan *e.ErrorInfo, 1000)
	rabbitmqChannel   *amqp.Channel
)

const shardCount = 64

func initRabbitmqQueue(cfg *config.Config) (<-chan amqp.Delivery, []string, *amqp.Channel, *e.ErrorInfo) {
	rabbitmqChannel, err := rabbitmq.NewRabbitmqChannel(cfg)
	if !err.IsNil() {
		return nil, nil, nil, err
	}

	// Reasonable default; per-session ordering is enforced by our handler goroutines anyway.
	_ = rabbitmqChannel.Qos(50, 0, false)

	merged := make(chan amqp.Delivery, 1000)
	consumerTags := make([]string, 0, shardCount)

	var forwardWg sync.WaitGroup
	forwardWg.Add(shardCount)

	podID := os.Getenv("POD_ID")
	if podID == "" {
		podID = "unknown"
	}

	for i := 0; i < shardCount; i++ {
		q := fmtShardQueue(i)
		tag := fmt.Sprintf("events-%s-%s", podID, q)
		consumerTags = append(consumerTags, tag)

		consumer, unwrappedError := rabbitmqChannel.Consume(
			q,
			tag,
			false, // manual acks
			false,
			false,
			false,
			amqp.Table{},
		)
		if unwrappedError != nil {
			_ = rabbitmqChannel.Close()
			return nil, nil, nil, e.FromError(unwrappedError, "failed to init consumer").WithSeverity(e.Critical).WithData(map[string]any{"queue": q})
		}

		go func(c <-chan amqp.Delivery) {
			defer forwardWg.Done()
			for d := range c {
				merged <- d
			}
		}(consumer)
	}

	go func() {
		forwardWg.Wait()
		close(merged)
	}()

	return merged, consumerTags, rabbitmqChannel, e.Nil()
}

type WorkerArgs struct {
	wg          *sync.WaitGroup
	ctx         context.Context
	updatesChan chan amqp.Delivery
	cfg         *config.Config
	sessionId   string
}

func ListenToRabbitmq(cfg *config.Config, ctx context.Context, wg *sync.WaitGroup) *e.ErrorInfo {
	var consumer <-chan amqp.Delivery
	var consumerTags []string
	var err *e.ErrorInfo
	consumer, consumerTags, rabbitmqChannel, err = initRabbitmqQueue(cfg)
	if !err.IsNil() {
		return err
	}
	// router is a package-level singleton and was initialized before rabbitmqChannel was set.
	// Update it to use the live channel to avoid nil deref in PublishWithContext.
	router.RabbitmqChannel = rabbitmqChannel
	defer rabbitmqChannel.Close()

	go hanleError(errors, ctx, wg)

	for {
		select {
		case <-ctx.Done():
			for _, tag := range consumerTags {
				_ = rabbitmqChannel.Cancel(tag, false)
			}
			for sessionId, ch := range gorutineEndpoints {
				delete(gorutineEndpoints, sessionId)
				close(ch)
			}
			return e.Nil()
		case sessionId := <-handlerTerminated:
			if ch, ok := gorutineEndpoints[sessionId]; ok {
				delete(gorutineEndpoints, sessionId)
				close(ch)
			}
		case delivery, ok := <-consumer:
			if !ok {
				// consumer channel closed
				// ToDo: Не подтверждать доставку
				return e.FromError(nil, "RabbitMQ consumer channel closed").WithSeverity(e.Critical)
			}
			initNewHandlerOrPushToExisting(delivery, ctx, wg, cfg)
		}
	}
}

func hanleError(src chan (*e.ErrorInfo), context context.Context, wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()

	for {
		select {
		case <-context.Done():
			return
		case err := <-src:
			log.Println(err.JSON())
		}
	}
}

func initNewHandlerOrPushToExisting(delivery amqp.Delivery, ctx context.Context, wg *sync.WaitGroup, cfg *config.Config) {
	sessionId, err := sessionIDFromHeaders(delivery.Headers)
	if !err.IsNil() {
		errors <- err.WithData(map[string]any{"rk": delivery.RoutingKey}).WithSeverity(e.Critical)
		return
	}

	if _, ok := gorutineEndpoints[sessionId]; ok {
		gorutineEndpoints[sessionId] <- delivery
	} else {
		destChan := initHandler(ctx, wg, cfg, sessionId)
		destChan <- delivery
		gorutineEndpoints[sessionId] = destChan
	}

	// close(delivery.Ack(false))
}

func sessionIDFromHeaders(h amqp.Table) (string, *e.ErrorInfo) {
	if h == nil {
		return "", e.NewError("missing headers", "delivery headers are nil")
	}
	v, ok := h["session_id"]
	if !ok || v == nil {
		return "", e.NewError("missing session_id", "delivery missing header session_id")
	}
	switch t := v.(type) {
	case string:
		if t == "" {
			return "", e.NewError("empty session_id", "session_id header is empty")
		}
		return t, e.Nil()
	case []byte:
		if len(t) == 0 {
			return "", e.NewError("empty session_id", "session_id header is empty")
		}
		return string(t), e.Nil()
	default:
		return "", e.NewError("invalid session_id header type", "session_id header has invalid type").WithData(map[string]any{"type": fmt.Sprintf("%T", v)})
	}
}

func fmtShardQueue(i int) string {
	return fmt.Sprintf("%s.q%02d", config.PodType, i)
}

func initHandler(ctx context.Context, wg *sync.WaitGroup, cfg *config.Config, sessionId string) chan amqp.Delivery {
	updatesChan := make(chan amqp.Delivery, 1000)

	wg.Add(1)
	go handle(WorkerArgs{
		wg:          wg,
		ctx:         ctx,
		updatesChan: updatesChan,
		cfg:         cfg,
		sessionId:   sessionId,
	})

	return updatesChan
}

func handle(args WorkerArgs) {
	defer args.wg.Done()

	timer := time.NewTimer(args.cfg.RuntimeConfig.HandlerLiveDuration)
	defer timer.Stop()

	for {
		select {
		case <-args.ctx.Done():
			// On shutdown the main loop may already be gone; don't block here.
			select {
			case handlerTerminated <- args.sessionId:
			default:
			}
			return
		case <-timer.C:
			handlerTerminated <- args.sessionId
			// TODO: Добавить подтверждение завершения обработки всех текущих апдейтов
			return
		case delivery, ok := <-args.updatesChan:
			if !ok {
				return
			}
			log.Printf("trace=%s received rk=%s", delivery.CorrelationId, delivery.RoutingKey)
			updateBytes := delivery.Body
			var update tele.Update
			unwrappedError := json.Unmarshal(updateBytes, &update)
			if unwrappedError != nil {
				errors <- e.FromError(unwrappedError, "failed to unmarshal update").WithSeverity(e.Critical)
				continue
			}
			router.Dispatch(update)
			// TODO: Сделать подтверждение зависимым от исхода обработки в Dispatch
			unwrappedError = delivery.Ack(false)

			if unwrappedError != nil {
				errors <- e.FromError(unwrappedError, "Ошибка подтверждения получения")
			}

			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(args.cfg.RuntimeConfig.HandlerLiveDuration)
		}
	}
}

var router Router = Router{
	ErrorChannel:    errors,
	RabbitmqChannel: rabbitmqChannel,
	Endpoints: []Endpoint{
		{
			handler: func(update tele.Update, timeout time.Duration) (handlerResponse, *e.ErrorInfo) {
				return handlerResponse{
					Method: "text",
					SendData: map[string]any{
						"text":    "New business message sent",
						"chat_id": update.BusinessMessage.Sender.ID,
					},
					SenderBot: "@main",
				}, e.Nil()
			},
			filter:  BusinessEvent(BusEventTypeNew),
			timeout: time.Second * 10,
			Name:    "test",
		},
	},
}
