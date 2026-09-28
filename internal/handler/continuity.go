package handler

import (
	"crypto/subtle"
	"fmt"
	"strings"

	"aurora/httpclient/bogdanfinn"
	"aurora/internal/accounts"
	"aurora/internal/continuity"
	chatgpttypes "aurora/typings/chatgpt"
	official "aurora/typings/official"

	"github.com/gin-gonic/gin"
)

// Domain separator, not a secret: persisted account IDs contain only hashes of
// account/workspace/user claims. Credentials never enter the state directory.
var conversationIdentityKey = []byte("aurora-conversation-account-v1")

func applyConversationLease(req *chatgpttypes.ChatGPTRequest, lease *continuity.Lease) {
	if lease == nil || lease.ConversationID == "" {
		return
	}
	req.ConversationID, req.ParentMessageID = lease.ConversationID, lease.ParentID
	// The converter aggregates fixed instructions into the first system message.
	// They already exist upstream; only the newly converted suffix belongs here.
	if len(req.Messages) > 0 && req.Messages[0].Author.Role == "system" {
		req.Messages = req.Messages[1:]
	}
}

func (h *ChatHandler) prepareConversation(c *gin.Context, req official.APIRequest) (*continuity.Lease, *accounts.Account, error) {
	user, hasUser := c.Request.Header["X-Aurora-User-Id"]
	conversation, hasConversation := c.Request.Header["X-Aurora-Conversation-Id"]
	if !hasUser && !hasConversation {
		return nil, nil, nil
	}
	if len(user) != 1 || len(conversation) != 1 || strings.TrimSpace(user[0]) == "" || strings.TrimSpace(conversation[0]) == "" {
		return nil, nil, fmt.Errorf("continuity requires one nonempty X-Aurora-User-Id and X-Aurora-Conversation-Id")
	}
	if strings.Contains(user[0], "{{") || strings.Contains(conversation[0], "{{") {
		return nil, nil, fmt.Errorf("continuity identity headers contain unresolved server templates")
	}
	// Continuity only accepts the authenticated gateway credential, not external
	// ChatGPT tokens, anonymous callers or middleware's optional token override.
	auth := strings.TrimPrefix(c.GetHeader("Authorization"), "Bearer ")
	if h.cfg.Authorization == "" || subtle.ConstantTimeCompare([]byte(auth), []byte(h.cfg.Authorization)) != 1 {
		return nil, nil, fmt.Errorf("continuity requires the configured gateway credential")
	}
	lease, err := h.continuity.Prepare(h.cfg.Authorization, user[0], conversation[0], req)
	if err != nil {
		return nil, nil, err
	}
	var account *accounts.Account
	if lease.Account != "" {
		account = h.accountPool.FindConversationAccount(lease.Account, conversationIdentityKey)
	} else {
		account, err = h.accountPool.Acquire(accounts.TypeFree)
		if err != nil {
			account, err = h.accountPool.Acquire(accounts.TypePUID)
		}
	}
	if err != nil || account == nil || accounts.ImageIdentity(account, conversationIdentityKey) == "" || account.Client == nil {
		lease.Close()
		return nil, nil, fmt.Errorf("continuity bound/configured account unavailable; no fallback is permitted")
	}
	if client, ok := account.Client.(*bogdanfinn.TlsClient); !ok || client == nil {
		lease.Close()
		return nil, nil, fmt.Errorf("continuity requires the configured account's TLS client")
	}
	return lease, account, nil
}

func commitConversation(lease *continuity.Lease, account *accounts.Account, conversation, parent, text string, calls []official.ToolCall) error {
	output := official.NewTextMessage("assistant", text)
	for _, call := range calls {
		ref := official.ToolCallRef{ID: call.ID, Type: call.Type, Index: call.Index}
		ref.Function.Name, ref.Function.Arguments = call.Function.Name, call.Function.Arguments
		output.ToolCalls = append(output.ToolCalls, ref)
	}
	return lease.Commit(accounts.ImageIdentity(account, conversationIdentityKey), conversation, parent, output)
}
