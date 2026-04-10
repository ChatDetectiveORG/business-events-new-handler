package endpoints

import (
	"app/src/infrastructure/postgresql"
	"time"

	e "github.com/ChatDetectiveORG/shared/errors"
	h "github.com/ChatDetectiveORG/shared/handlers"
	models "github.com/ChatDetectiveORG/shared/postgresModels"
	utils "github.com/ChatDetectiveORG/shared/utils"
	tele "gopkg.in/telebot.v4"
)

func NewSaveEndpoint() h.Endpoint {
	ep := h.Endpoint{}
	ep.Init(
		"save",
		*h.HandlerChain{}.Init(
			10 * time.Second,
			h.InitChainHandler(saveMessage, h.EndOnError),
		),
		h.BusinessEvent(h.BusEventTypeNew),
	)

	return ep
}

func saveMessage(update tele.Update, hashe *h.HandlerChainHashe) *e.ErrorInfo {
	message := &models.Message{
		SenderID: utils.Int64ToHash(update.BusinessMessage.Sender.ID),
		ChatID: utils.Int64ToHash(update.BusinessMessage.Chat.ID),
		MessageID: update.BusinessMessage.ID,
		BusinessConnectionID: update.BusinessMessage.BusinessConnectionID,
		Metadata: update.BusinessMessage,
	}

	db := postgresql.GetDB()
	_, err := db.Model(message).Insert()
	if e.IsNonNil(err) {
		return e.FromError(err, "failed to insert message")
	}

	return e.Nil()
}
