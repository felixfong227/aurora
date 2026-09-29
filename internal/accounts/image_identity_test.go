package accounts

import (
	"encoding/base64"
	"testing"

	"aurora/httpclient/bogdanfinn"
)

func TestIdentityAccountBinding(t *testing.T) {
	token := "fixture." + base64.RawURLEncoding.EncodeToString([]byte(`{"https://api.openai.com/auth":{"chatgpt_account_id":"workspace","chatgpt_user_id":"user"}}`)) + ".unsigned"
	account := NewAccount("fixture", TypeFree, token)
	account.Status = StatusActive
	account.Client = bogdanfinn.NewStdClient()
	pool := NewPool([]*Account{account})
	key := []byte("test-key")
	identity := ImageIdentity(account, key)

	bound := pool.FindConversationAccount(identity, key)
	if bound != account || bound.Client != account.Client {
		t.Fatal("conversation lookup must return the configured account pointer")
	}
	image := pool.FindImageAccount(identity, key)
	if image == nil || image == account || image.Client != nil || image.WSSActor != nil {
		t.Fatal("image lookup must remain detached without client or actor")
	}
	image.Status = StatusDisabled
	if account.Status != StatusActive {
		t.Fatal("image snapshot mutated the pool")
	}
	pool.ReportFailure(bound)
	if account.Status != StatusExpired || account.FailedCalls != 1 || len(pool.ExpiredAccounts()) != 1 {
		t.Fatal("bound account failure did not reach the pool")
	}
	if _, err := pool.Acquire(TypeFree); err == nil || pool.FindConversationAccount(identity, key) != nil {
		t.Fatal("expired bound account remained available")
	}
	// Renewal updates the configured pointer, not a stale lookup snapshot.
	account.Status = StatusActive
	account.Token = token + "-renewed"
	if bound.Token != account.Token || pool.FindConversationAccount(identity, key) != bound {
		t.Fatal("bound account did not observe renewal")
	}
}
