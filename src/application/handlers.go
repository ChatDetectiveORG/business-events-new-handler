package application

import (
	e "app/pkg/errors"

	amqp "github.com/rabbitmq/amqp091-go"
	tele "gopkg.in/telebot.v4"
)

type Router struct {
	Endpoints []Endpoint
	ErrorChannel chan *e.ErrorInfo
	RabbitmqChannel *amqp.Channel
}

func (r *Router) Dispatch(update tele.Update) *e.ErrorInfo {
	for _, endpoint := range r.Endpoints {
		err := endpoint.ExecuteIfFilterPasses(update, r.RabbitmqChannel)
		if !err.IsNil() {
			r.ErrorChannel <- err.PushStack().WithData(map[string]any{"endpoint name": endpoint.Name})
		}
	}

	return nil
}
