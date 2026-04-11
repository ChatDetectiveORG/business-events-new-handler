package endpoints

import (
	"app/src/infrastructure/postgresql"
	"encoding/json"
	"strconv"
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
	if update.BusinessMessage.Sender.ID != update.BusinessMessage.Chat.ID {
		// Отправитель - пользователь бота. Можно обновлять данные.
		db := postgresql.GetDB()
		user := &models.Telegramuser{}
		err := user.GetByTelegramID(db, update.BusinessMessage.Sender.ID)
		if e.IsNonNil(err) {
			return e.FromError(err, "failed to get user by telegram id")
		}

		user.BusinessConnectionIDHash = utils.ToHash(update.BusinessMessage.BusinessConnectionID)

		_, eraw := db.Model(user).WherePK().Column("business_connection_id_hash").Update()
		if e.IsNonNil(eraw) {
			return e.FromError(eraw, "failed to update user business connection id hash")
		}		
	}

	user := &models.Telegramuser{
		BusinessConnectionIDHash: utils.ToHash(update.BusinessMessage.BusinessConnectionID),
	}
	db := postgresql.GetDB()
	err := db.Model(user).
		Where("business_connection_id_hash = ?", user.BusinessConnectionIDHash).
		Select()
	if e.IsNonNil(err) {
		return e.FromError(err, "failed to select user")
	}

	key, err := utils.DecryptUserKey(user.DataEncryptionKey)
	if e.IsNonNil(err) {
		return e.FromError(err, "failed to decrypt user key")
	}

	encryptedId, err := utils.Encrypt([]byte(strconv.FormatInt(update.BusinessMessage.Sender.ID, 10)), key)
	if e.IsNonNil(err) {
		return e.FromError(err, "failed to encrypt sender id")
	}

	encryptedChatId, err := utils.Encrypt([]byte(strconv.FormatInt(update.BusinessMessage.Chat.ID, 10)), key)
	if e.IsNonNil(err) {
		return e.FromError(err, "failed to encrypt chat id")
	}

	jsonMetadata, eraw := json.Marshal(update.BusinessMessage)
	if e.IsNonNil(eraw) {
		return e.FromError(eraw, "failed to encrypt message metadata")
	}

	encryptedMetadata, err := utils.Encrypt(jsonMetadata, key)
	if e.IsNonNil(err) {
		return e.FromError(err, "failed to encrypt message metadata")
	}

	message := &models.Message{
		SenderID: encryptedId,
		ChatID: encryptedChatId,
		MessageID: update.BusinessMessage.ID,
		BusinessConnectionIDHash: utils.ToHash(update.BusinessMessage.BusinessConnectionID),
		Metadata: encryptedMetadata,
	}

	_, err = db.Model(message).Insert()
	if e.IsNonNil(err) {
		return e.FromError(err, "failed to insert message")
	}

	return e.Nil()
}
