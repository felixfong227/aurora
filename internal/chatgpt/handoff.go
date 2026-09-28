package chatgpt

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"aurora/httpclient"
	"aurora/internal/accounts"
)

var ErrIncompleteHandoff = errors.New("ChatGPT's background response could not be retrieved. Check the existing conversation in ChatGPT before retrying; it may already have completed.")

// completedConversationSSE replays only the active branch after its latest user
// message. A transport DONE after stream_handoff is not a completed ChatGPT turn.
func completedConversationSSE(conversationID, expectedUserID string, conversation map[string]interface{}) ([]byte, bool, error) {
	mapping, _ := conversation["mapping"].(map[string]interface{})
	current, _ := conversation["current_node"].(string)
	node, _ := mapping[current].(map[string]interface{})
	message, _ := node["message"].(map[string]interface{})
	author, _ := message["author"].(map[string]interface{})
	ended, _ := message["end_turn"].(bool)
	if current == "" || node == nil {
		return nil, false, nil
	}
	content, _ := message["content"].(map[string]interface{})
	complete := author["role"] == "assistant" && ended &&
		content["content_type"] != "thoughts" && content["content_type"] != "reasoning_recap"
	if status, _ := message["status"].(string); complete && status != "" && status != "finished_successfully" {
		return nil, false, fmt.Errorf("upstream turn did not complete successfully")
	}
	var messages []map[string]interface{}
	seen := make(map[string]bool)
	foundUser := false
	for current != "" {
		if seen[current] {
			return nil, false, fmt.Errorf("cycle in upstream conversation")
		}
		seen[current] = true
		node, ok := mapping[current].(map[string]interface{})
		if !ok {
			return nil, false, fmt.Errorf("incomplete upstream conversation branch")
		}
		message, _ := node["message"].(map[string]interface{})
		author, _ := message["author"].(map[string]interface{})
		if author["role"] == "user" {
			if expectedUserID == "" || message["id"] != expectedUserID {
				if !complete {
					return nil, false, nil
				}
				return nil, false, fmt.Errorf("upstream response belongs to a different user turn")
			}
			foundUser = true
			break
		}
		if author["role"] == "assistant" || author["role"] == "tool" {
			messages = append(messages, message)
		}
		current, _ = node["parent"].(string)
	}
	if !foundUser {
		return nil, false, fmt.Errorf("upstream current turn has no user message")
	}
	var body bytes.Buffer
	for i := len(messages) - 1; i >= 0; i-- {
		frame, err := json.Marshal(map[string]interface{}{"conversation_id": conversationID, "message": messages[i]})
		if err != nil {
			return nil, false, err
		}
		fmt.Fprintf(&body, "data: %s\n\n", frame)
	}
	if complete {
		body.WriteString("data: [DONE]\n\n")
	}
	return body.Bytes(), complete, nil
}

func getCompletedConversation(ctx context.Context, client httpclient.AuroraHttpClient, account *accounts.Account, conversationID, expectedUserID string, beforePoll func(), onProgress func([]byte)) ([]byte, error) {
	if client == nil || conversationID == "" || expectedUserID == "" {
		return nil, fmt.Errorf("missing handoff conversation")
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if beforePoll != nil {
			beforePoll()
		}
		conversation, err := getConversationWithContext(ctx, client, account, conversationID)
		if err != nil {
			return nil, err
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		body, complete, err := completedConversationSSE(conversationID, expectedUserID, conversation)
		if err != nil {
			return nil, err
		}
		if onProgress != nil && len(body) > 0 {
			onProgress(body)
		}
		if complete {
			return body, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}
