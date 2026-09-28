package accounts

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

// ImageIdentity is stable across process restarts and access-token renewal.
// Claims identify a configured account; this is not authentication of client JWTs.
// Include the user as well as the workspace to keep team members separate.
func ImageIdentity(account *Account, key []byte) string {
	if account == nil || account.IsTemporary || account.Type == TypeNoAuth || len(key) == 0 {
		return ""
	}
	accountID := ExtractChatGPTAccountID(account.Token)
	userID := ExtractChatGPTUserID(account.Token)
	if accountID == "" || userID == "" {
		return ""
	}
	scope, _ := json.Marshal([]string{accountID, userID, account.TeamUserID})
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte("aurora-image-owner-v1\n"))
	mac.Write(scope)
	return hex.EncodeToString(mac.Sum(nil))
}

// FindImageAccount returns a detached snapshot, never a randomly selected account.
// Removing/disabling the configured account revokes its image links.
func (p *Pool) FindImageAccount(identity string, key []byte) *Account {
	if identity == "" {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	// ponytail: scan the configured pool; index identities if large pools make this costly.
	for _, group := range [][]*Account{p.free, p.puid} {
		for _, account := range group {
			if account.Status != StatusActive || ImageIdentity(account, key) != identity {
				continue
			}
			snapshot := *account
			snapshot.Client = nil
			snapshot.WSSActor = nil
			return &snapshot
		}
	}
	return nil
}
