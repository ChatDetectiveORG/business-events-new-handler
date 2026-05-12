package endpoints

import (
	"bytes"
	"encoding/json"
	"strconv"
	"time"

	"github.com/ChatDetectiveORG/business-events-new-handler/src/infrastructure/postgresql"

	"github.com/go-pg/pg/v10"
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
			10*time.Second,
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

		user.BusinessConnectionIDHash, err = utils.ToSecureHash(update.BusinessMessage.BusinessConnectionID)
		if e.IsNonNil(err) {
			return e.FromError(err, "failed to get secure hash")
		}

		_, eraw := db.Model(user).WherePK().Column("business_connection_id_hash").Update()
		if e.IsNonNil(eraw) {
			return e.FromError(eraw, "failed to update user business connection id hash")
		}
	}

	businessConnectionIDHash, err := utils.ToSecureHash(update.BusinessMessage.BusinessConnectionID)
	if e.IsNonNil(err) {
		return e.FromError(err, "failed to get secure hash")
	}

	user := &models.Telegramuser{
		BusinessConnectionIDHash: businessConnectionIDHash,
	}
	db := postgresql.GetDB()
	eraw := db.Model(user).
		Where("business_connection_id_hash = ?", user.BusinessConnectionIDHash).
		Select()
	if e.IsNonNil(eraw) {
		return e.FromError(eraw, "failed to select user").WithData(map[string]any{"business_connection_id_hash": businessConnectionIDHash})
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

	chatIDHash, err := utils.ToSecureHash(update.BusinessMessage.Chat.ID)
	if e.IsNonNil(err) {
		return e.FromError(err, "failed to get secure hash")
	}

	senderIdHash, err := utils.ToSecureHash(update.BusinessMessage.Sender.ID)
	if e.IsNonNil(err) {
		return e.FromError(err, "failed to get secure hash")
	}

	message := &models.Message{
		SenderID:                 encryptedId,
		ChatID:                   encryptedChatId,
		ChatIDHash:               chatIDHash,
		MessageID:                update.BusinessMessage.ID,
		BusinessConnectionIDHash: businessConnectionIDHash,
		Metadata:                 encryptedMetadata,
		SenderIDHash:             senderIdHash,
	}

	if update.BusinessMessage.AlbumID != "" {
		mediaGroupIDHash, err := utils.ToSecureHash(update.BusinessMessage.AlbumID)
		if e.IsNonNil(err) {
			return e.FromError(err, "failed to get secure hash")
		}

		message.MediaGroupIDHash = mediaGroupIDHash
	}

	_, eraw = db.Model(message).Insert()
	if e.IsNonNil(eraw) {
		return e.FromError(eraw, "failed to insert message")
	}

	if update.BusinessMessage.Private() {
		// relate business account owner (db user) ↔ private chat peer (Chat.ID).
		// Sender.ID often equals Chat.ID for inbound customer messages, so Sender↔Chat was a no-op (tgID1 == tgID2).
		ownerTgID, err := user.GetTgId()
		if e.IsNonNil(err) {
			return err.PushStack()
		}
		if err := ensurePrivateChatUserRelation(db, ownerTgID, update.BusinessMessage.Chat.ID); e.IsNonNil(err) {
			return err
		}
	}

	return e.Nil()
}

// ensurePrivateChatUserRelation records that two registered users may know each other (private chat).
// Both must already exist as Telegramuser rows; missing either side is ignored.
func ensurePrivateChatUserRelation(db *pg.DB, tgID1, tgID2 int64) *e.ErrorInfo {
	if tgID1 == tgID2 {
		return e.Nil()
	}
	u1 := &models.Telegramuser{}
	if err := u1.GetByTelegramID(db, tgID1); e.IsNonNil(err) {
		return e.Nil()
	}
	u2 := &models.Telegramuser{}
	if err := u2.GetByTelegramID(db, tgID2); e.IsNonNil(err) {
		return e.Nil()
	}
	var first, second []byte
	if bytes.Compare(u1.ID, u2.ID) < 0 {
		first, second = u1.ID, u2.ID
	} else {
		first, second = u2.ID, u1.ID
	}
	n, eraw := db.Model((*models.UserRelations)(nil)).
		Where("first_user_id = ? AND second_user_id = ?", first, second).
		Count()
	if e.IsNonNil(eraw) {
		return e.FromError(eraw, "failed to count user relations")
	}
	if n > 0 {
		return e.Nil()
	}
	rel := &models.UserRelations{
		FirstUserID:  first,
		SecondUserID: second,
	}
	_, eraw = db.Model(rel).Insert()
	if e.IsNonNil(eraw) {
		return e.FromError(eraw, "failed to insert user relation")
	}
	return e.Nil()
}
