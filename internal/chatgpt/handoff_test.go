package chatgpt

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	"aurora/httpclient"
	"aurora/internal/httpstream"

	"github.com/gin-gonic/gin"
)

const handoffTestUserID = "00000000-0000-4000-8000-000000000001"

// The only allowed operations are reading the existing conversation and resolving
// the current image URL. No generation, WebSocket, or image-byte requests escape.
type handoffTestClient struct {
	httpclient.AuroraHttpClient
	t                 *testing.T
	snapshot          map[string]interface{}
	getErr            error
	getStatus         int
	getBody           io.ReadCloser
	conversationReads int
	imageReads        int
}

func (c *handoffTestClient) Request(method httpclient.HttpMethod, url string, headers httpclient.AuroraHeaders, cookies []*http.Cookie, body io.Reader) (*http.Response, error) {
	if method != http.MethodGet || body != nil {
		c.t.Fatalf("recovery must be GET-only, got %s %s", method, url)
	}
	switch url {
	case BaseURL + "/conversation/conv-handoff-test":
		c.conversationReads++
		if c.conversationReads > 1 {
			c.t.Fatal("completed snapshot should not require another poll")
		}
		if c.getErr != nil {
			return nil, c.getErr
		}
		status := c.getStatus
		if status == 0 {
			status = http.StatusOK
		}
		if c.getBody != nil {
			return &http.Response{StatusCode: status, Body: c.getBody}, nil
		}
		payload, err := json.Marshal(c.snapshot)
		if err != nil {
			return nil, err
		}
		return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(string(payload)))}, nil
	case fileDownloadBaseURL() + "file-handoffcurrent/download":
		c.imageReads++
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"status":"success","download_url":"https://example.invalid/handoff-current.png"}`))}, nil
	default:
		c.t.Fatalf("unexpected recovery request: %s %s", method, url)
		return nil, fmt.Errorf("unexpected recovery request")
	}
}

func handoffTestSnapshot(t *testing.T, image bool) map[string]interface{} {
	t.Helper()
	var snapshot map[string]interface{}
	// The old assistant is on the ancestry path; the sibling is a different
	// completed branch of this same user turn. Neither belongs in the replay.
	err := json.Unmarshal([]byte(`{
		"conversation_id":"conv-handoff-test","current_node":"current-answer",
		"mapping":{
			"old-user":{"id":"old-user","parent":null,"children":["old-answer"],"message":{"id":"old-user","author":{"role":"user"},"content":{"content_type":"text","parts":["Synthetic prior turn"]}}},
			"old-answer":{"id":"old-answer","parent":"old-user","children":["00000000-0000-4000-8000-000000000001"],"message":{"id":"old-answer","author":{"role":"assistant"},"recipient":"all","content":{"content_type":"image_asset_pointer","parts":[{"asset_pointer":"sediment://file-handoff-old"}]},"end_turn":true,"metadata":{"message_type":"next","image_gen_task_id":"task-old","dalle":{"gen_id":"gen-old","slot_index":1}}}},
			"00000000-0000-4000-8000-000000000001":{"id":"00000000-0000-4000-8000-000000000001","parent":"old-answer","children":["current-answer","sibling-answer"],"message":{"id":"00000000-0000-4000-8000-000000000001","author":{"role":"user"},"content":{"content_type":"text","parts":["Synthetic current turn"]}}},
			"sibling-answer":{"id":"sibling-answer","parent":"00000000-0000-4000-8000-000000000001","children":[],"message":{"id":"sibling-answer","author":{"role":"assistant"},"recipient":"all","content":{"content_type":"image_asset_pointer","parts":[{"asset_pointer":"sediment://file-handoff-sibling"}]},"end_turn":true,"metadata":{"message_type":"next","image_gen_task_id":"task-sibling","dalle":{"gen_id":"gen-sibling","slot_index":1}}}},
			"current-answer":{"id":"current-answer","parent":"00000000-0000-4000-8000-000000000001","children":[],"message":{"id":"current-answer","author":{"role":"assistant"},"recipient":"all","content":{"content_type":"text","parts":["A synthetic title"]},"end_turn":true,"metadata":{"message_type":"next","finish_details":{"type":"stop"}}}}
		}
	}`), &snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if image {
		mapping := snapshot["mapping"].(map[string]interface{})
		answer := mapping["current-answer"].(map[string]interface{})
		answer["parent"] = "current-image"
		answer["message"].(map[string]interface{})["content"] = map[string]interface{}{"content_type": "text", "parts": []interface{}{""}}
		mapping[handoffTestUserID].(map[string]interface{})["children"] = []interface{}{"current-image", "sibling-answer"}
		mapping["current-image"] = map[string]interface{}{
			"id": "current-image", "parent": handoffTestUserID, "children": []interface{}{"current-answer"},
			"message": map[string]interface{}{
				"id": "current-image", "author": map[string]interface{}{"role": "tool", "name": "image_gen"}, "recipient": "all", "end_turn": false,
				"content":  map[string]interface{}{"content_type": "image_asset_pointer", "parts": []interface{}{map[string]interface{}{"asset_pointer": "sediment://file-handoffcurrent"}}},
				"metadata": map[string]interface{}{"message_type": "next", "image_gen_task_id": "task-current", "dalle": map[string]interface{}{"gen_id": "gen-current", "slot_index": 1}},
			},
		}
	}
	return snapshot
}

func runHandoffTestHandler(t *testing.T, client *handoffTestClient, stream bool) (HandlerResult, *httptest.ResponseRecorder, *gin.Context) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	request := chatGPTRequestForTest()
	if err := json.Unmarshal([]byte(`{"conversation_id":"conv-handoff-test","parent_message_id":"old-answer","messages":[{"id":"00000000-0000-4000-8000-000000000001","author":{"role":"user"},"content":{"content_type":"text","parts":["Synthetic current turn"]}}]}`), &request); err != nil {
		t.Fatal(err)
	}
	body := `data: {"conversation_id":"conv-handoff-test","message":{"id":"00000000-0000-4000-8000-000000000001","author":{"role":"user"},"content":{"content_type":"text","parts":["Synthetic current turn"]}}}` + "\n\n" +
		`data: {"type":"stream_handoff","options":[{"type":"subscribe_ws_topic","topic_id":"conversation-turn-handoff-test"}]}` + "\n\ndata: [DONE]\n\n"
	response := &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body))}
	defer response.Body.Close()
	writer := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(writer)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	result := HandlerDetailedWithOptions(c, response, client, nil, "request-handoff-test", request, stream, "auto", HandlerDetailedOptions{ArtifactDelivery: ArtifactDeliveryURL})
	return result, writer, c
}

func TestHandlerRecoversHandoffTitleWithoutWebsocket(t *testing.T) {
	client := &handoffTestClient{t: t, snapshot: handoffTestSnapshot(t, false)}
	result, writer, _ := runHandoffTestHandler(t, client, false)
	if result.Text != "A synthetic title" || result.ConversationID != "conv-handoff-test" || result.ParentMessageID != "current-answer" || result.Continue != nil {
		t.Fatalf("recovered title = %#v", result)
	}
	if client.conversationReads != 1 || client.imageReads != 0 {
		t.Fatalf("GET counts: conversation=%d image=%d", client.conversationReads, client.imageReads)
	}
	if writer.Code != http.StatusOK || writer.Body.Len() != 0 {
		t.Fatalf("nonstream handler should return text for its caller, got %d %s", writer.Code, writer.Body.String())
	}
}

func TestHandlerRecoversHandoffImageStreamWithoutWebsocket(t *testing.T) {
	client := &handoffTestClient{t: t, snapshot: handoffTestSnapshot(t, true)}
	result, writer, c := runHandoffTestHandler(t, client, true)
	image := regexp.MustCompile(`!\[[^\]\r\n]*\]\(https://example\.invalid/handoff-current\.png(?: "[^"\r\n]*")?\)`)
	if len(image.FindAllString(result.Text, -1)) != 1 || strings.Count(result.Text, "![") != 1 {
		t.Fatalf("recovered content must contain exactly one Markdown image, got %q", result.Text)
	}
	if strings.Contains(result.Text, "file-handoff-old") || strings.Contains(result.Text, "file-handoff-sibling") || strings.Contains(result.Text, "Synthetic current turn") {
		t.Fatalf("replayed unrelated or user content: %q", result.Text)
	}
	if result.ConversationID != "conv-handoff-test" || result.ParentMessageID != "current-answer" || result.Continue != nil {
		t.Fatalf("recovered turn identity = %#v", result)
	}
	if client.conversationReads != 1 || client.imageReads != 1 {
		t.Fatalf("GET counts: conversation=%d image=%d", client.conversationReads, client.imageReads)
	}
	httpstream.WriteChatCompletionDone(c, result.StopSent, "auto", result.ConversationID)
	assertCompletedChatStream(t, writer.Body.String(), result.Text, "stop")
}

type handoffReadError struct{}

func (handoffReadError) Read([]byte) (int, error) { return 0, errors.New("synthetic read failure") }
func (handoffReadError) Close() error             { return nil }

func TestHandlerRecoversHandoffGETFailureIsExplicit(t *testing.T) {
	for _, failure := range []string{"transport", "status", "read"} {
		t.Run(failure, func(t *testing.T) {
			client := &handoffTestClient{t: t, snapshot: handoffTestSnapshot(t, false)}
			switch failure {
			case "transport":
				client.getErr = errors.New("synthetic GET failure")
			case "status":
				client.getStatus = http.StatusServiceUnavailable
			case "read":
				client.getBody = handoffReadError{}
			}
			result, writer, _ := runHandoffTestHandler(t, client, false)
			if !errors.Is(result.Err, ErrIncompleteHandoff) || writer.Body.Len() != 0 {
				t.Fatalf("failed handoff must propagate error without writing the caller's protocol: err=%v body=%s", result.Err, writer.Body.String())
			}
			if client.conversationReads != 1 || result.Text != "" || result.Continue != nil {
				t.Fatalf("failed recovery must not produce an answer or retry generation: reads=%d result=%#v", client.conversationReads, result)
			}
		})
	}
}

func TestCompletedConversationSSEPendingCurrentUser(t *testing.T) {
	snapshot := handoffTestSnapshot(t, false)
	snapshot["current_node"] = handoffTestUserID
	data, completed, err := completedConversationSSE("conv-handoff-test", handoffTestUserID, snapshot)
	if err != nil || completed || len(data) != 0 {
		t.Fatalf("current user with old finished ancestors is pending: completed=%v error=%v data=%s", completed, err, data)
	}
}

func TestGetCompletedConversationCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	client := &handoffTestClient{t: t, snapshot: handoffTestSnapshot(t, false)}
	data, err := getCompletedConversation(ctx, client, nil, "conv-handoff-test", handoffTestUserID)
	if !errors.Is(err, context.Canceled) || len(data) != 0 {
		t.Fatalf("canceled recovery: error=%v data=%s", err, data)
	}
	if client.conversationReads != 0 {
		t.Fatalf("already canceled recovery made %d requests", client.conversationReads)
	}
}

func TestHandlerRecoversHandoffReaderFailure(t *testing.T) {
	for _, ending := range []string{"eof", "read_error", "unterminated_handoff"} {
		t.Run(ending, func(t *testing.T) {
			client := &handoffTestClient{t: t, snapshot: handoffTestSnapshot(t, false)}
			body := "data: {\"conversation_id\":\"conv-handoff-test\"}\n\ndata: {\"type\":\"stream_handoff\",\"options\":[{\"type\":\"subscribe_ws_topic\",\"topic_id\":\"test-topic\"}]}\n\n"
			if ending == "unterminated_handoff" {
				body = strings.TrimRight(body, "\n")
			}
			var reader io.Reader = strings.NewReader(body)
			if ending == "read_error" {
				reader = io.MultiReader(reader, handoffReadError{})
			}
			response := &http.Response{Body: io.NopCloser(reader)}
			writer := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(writer)
			request := chatGPTRequestForTest()
			if err := json.Unmarshal([]byte(`{"messages":[{"id":"00000000-0000-4000-8000-000000000001","author":{"role":"user"}}]}`), &request); err != nil {
				t.Fatal(err)
			}
			result := HandlerDetailedWithOptions(c, response, client, nil, "offline", request, false, "auto", HandlerDetailedOptions{})
			if result.Text != "A synthetic title" || client.conversationReads != 1 {
				t.Fatalf("reader failure lost completed answer: text=%q reads=%d", result.Text, client.conversationReads)
			}
		})
	}
}

func TestCompletedConversationSSERejectsDifferentTurn(t *testing.T) {
	data, complete, err := completedConversationSSE("conv-handoff-test", "different-user", handoffTestSnapshot(t, false))
	if err == nil || complete || len(data) != 0 {
		t.Fatal("replayed another turn")
	}
}

func TestHandlerDoesNotReplayContinuationOrPartialAnswer(t *testing.T) {
	for _, mode := range []string{"continue", "partial_nonstream", "partial_stream"} {
		t.Run(mode, func(t *testing.T) {
			client := &handoffTestClient{t: t, snapshot: handoffTestSnapshot(t, false)}
			request := chatGPTRequestForTest()
			request.Action = "continue"
			body := "data: {\"conversation_id\":\"conv-handoff-test\"}\n\n"
			if mode != "continue" {
				request.Action = "next"
				body += "data: {\"choices\":[{\"delta\":{\"role\":\"assistant\",\"content\":\"partial\"}}]}\n\n"
			}
			body += "data: {\"type\":\"stream_handoff\",\"options\":[{\"type\":\"subscribe_ws_topic\",\"topic_id\":\"offline\"}]}\n\ndata: [DONE]\n\n"
			writer := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(writer)
			response := &http.Response{Body: io.NopCloser(strings.NewReader(body))}
			result := HandlerDetailedWithOptions(c, response, client, nil, "offline", request, mode == "partial_stream", "auto", HandlerDetailedOptions{})
			if !errors.Is(result.Err, ErrIncompleteHandoff) || client.conversationReads != 0 {
				t.Fatalf("unsafe handoff replayed: err=%v reads=%d", result.Err, client.conversationReads)
			}
			if strings.Contains(writer.Body.String(), "[DONE]") || strings.Contains(writer.Body.String(), `"finish_reason":"stop"`) {
				t.Fatal("failed handoff was marked complete")
			}
		})
	}
}
