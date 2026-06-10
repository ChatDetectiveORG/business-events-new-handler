package endpoints

import (
	"bytes"
	"encoding/json"
	"log"
	"strconv"
	"time"

	"github.com/ChatDetectiveORG/business-events-new-handler/src/infrastructure/postgresql"

	e "github.com/ChatDetectiveORG/shared/errors"
	h "github.com/ChatDetectiveORG/shared/handlers"
	models "github.com/ChatDetectiveORG/shared/postgresModels"
	utils "github.com/ChatDetectiveORG/shared/utils"
	"github.com/go-pg/pg/v10"
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
	db := postgresql.GetDB()

	user, err := models.ResolveBotUserByBusinessConnection(db, update.BusinessMessage.BusinessConnectionID, update.BusinessMessage)
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

	chatIDHash, err := utils.ToSecureHash(update.BusinessMessage.Chat.ID)
	if e.IsNonNil(err) {
		return e.FromError(err, "failed to get secure hash")
	}

	senderIdHash, err := utils.ToSecureHash(update.BusinessMessage.Sender.ID)
	if e.IsNonNil(err) {
		return e.FromError(err, "failed to get secure hash")
	}

	businessConnectionIDHash, err := utils.ToSecureHash(update.BusinessMessage.BusinessConnectionID)
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
		if err := ensurePrivateChatUserRelation(db, user, &tele.User{
			ID: update.BusinessMessage.Chat.ID,
			FirstName: update.BusinessMessage.Chat.FirstName,
			LastName: update.BusinessMessage.Chat.LastName,
			Username: update.BusinessMessage.Chat.Username,
			Usernames: []string{update.BusinessMessage.Chat.Username},
		}); e.IsNonNil(err) {
			return err
		}
	} else {
		log.Println("Message save process: message is not private")
	}

	return e.Nil()
}

// ensurePrivateChatUserRelation records that two registered users may know each other (private chat).
// Both must already exist as Telegramuser rows; missing either side is ignored.
func ensurePrivateChatUserRelation(db *pg.DB, botUserModel *models.Telegramuser, interlocutor *tele.User) *e.ErrorInfo {
	interlocutorModel := &models.Telegramuser{}
	tx, eRaw := db.Begin()
	defer tx.Rollback()

	if e.IsNonNil(eRaw) {
		return e.FromError(eRaw, "failed to begin transaction")
	}
	if _, err := interlocutorModel.GetOrCreate(tx, interlocutor); e.IsNonNil(err) {
		return e.Nil()
	}

	if bytes.Equal(botUserModel.ID, interlocutorModel.ID) {
		return e.Nil()
	}

	var first, second []byte
	if bytes.Compare(botUserModel.ID, interlocutorModel.ID) < 0 {
		first, second = botUserModel.ID, interlocutorModel.ID
	} else {
		first, second = interlocutorModel.ID, botUserModel.ID
	}

	n, eraw := tx.Model((*models.UserRelations)(nil)).
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
	_, eraw = tx.Model(rel).Insert()
	if e.IsNonNil(eraw) {
		return e.FromError(eraw, "failed to insert user relation")
	}

	if eRaw = tx.Commit(); e.IsNonNil(eRaw) {
		return e.FromError(eRaw, "failed to commit transaction")
	}

	return e.Nil()
}
