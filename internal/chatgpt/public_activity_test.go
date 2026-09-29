package chatgpt

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"aurora/internal/httpstream"

	"github.com/gin-gonic/gin"
)

func TestHandlerPreservesPublicReasoningAndSnapshotCitations(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, envelope := range []bool{false, true} {
		for _, stream := range []bool{false, true} {
			t.Run(fmt.Sprintf("envelope=%t/stream=%t", envelope, stream), func(t *testing.T) {
				frames := []string{
					`{"conversation_id":"conv","message":{"id":"thought","author":{"role":"assistant"},"content":{"content_type":"thoughts","thoughts":[{"summary":"Checking documentation","content":"PRIVATE_REASONING","chunks":["PRIVATE_CHUNK"]}]},"end_turn":false}}`,
					`{"conversation_id":"conv","message":{"id":"thought","author":{"role":"assistant"},"content":{"content_type":"thoughts","thoughts":[{"summary":"Checking documentation","content":"PRIVATE_REASONING","chunks":["PRIVATE_CHUNK"]}]},"end_turn":false}}`,
					`{"conversation_id":"conv","message":{"id":"web-call","author":{"role":"assistant"},"recipient":"web.run","content":{"content_type":"text","parts":["PRIVATE_TOOL_ARGUMENTS"]},"end_turn":false,"metadata":{"message_type":"next"}}}`,
					`{"conversation_id":"conv","message":{"id":"recap","author":{"role":"assistant"},"content":{"content_type":"reasoning_recap","content":"Thought for 3m 38s"},"end_turn":true}}`,
					`{"conversation_id":"conv","message":{"id":"answer","author":{"role":"assistant"},"recipient":"all","content":{"content_type":"text","parts":["Read this citeturn1search0."]},"end_turn":true,"metadata":{"message_type":"next","content_references":[{"matched_text":"citeturn1search0","alt":"[Documentation](https://example.invalid/docs)","type":"grouped_webpages"}]}}}`,
				}
				var input strings.Builder
				for _, frame := range frames {
					if envelope {
						frame = `{"v":` + frame + `}`
					}
					fmt.Fprintf(&input, "data: %s\n\n", frame)
				}
				input.WriteString("data: [DONE]\n\n")
				writer := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(writer)
				result := HandlerDetailed(c, &http.Response{Body: io.NopCloser(strings.NewReader(input.String()))}, nil, nil, "test", chatGPTRequestForTest(), stream, "gpt-6-pro")
				want := "Read this [Documentation](https://example.invalid/docs)."
				if result.Text != want {
					t.Errorf("answer = %q, want citation-bearing final answer", result.Text)
				}
				for _, visible := range []string{"Checking documentation", "Searching the web.", "Thought for 3m 38s"} {
					if strings.Count(result.ThinkingText, visible) != 1 {
						t.Errorf("public activity %q missing or duplicated: %q", visible, result.ThinkingText)
					}
				}
				if strings.Contains(result.Text+result.ThinkingText+writer.Body.String(), "PRIVATE_") {
					t.Fatal("private reasoning/tool content leaked")
				}
				if !stream {
					if writer.Body.Len() != 0 {
						t.Fatal("nonstream handler wrote to response")
					}
					return
				}
				httpstream.WriteChatCompletionDone(c, result.StopSent, "gpt-6-pro", result.ConversationID)
				var reasoning strings.Builder
				for _, chunk := range parseSSEChunks(t, writer.Body.String()) {
					raw, _ := json.Marshal(chunk["choices"])
					var choices []struct {
						Delta struct {
							Reasoning string `json:"reasoning_content"`
						} `json:"delta"`
					}
					json.Unmarshal(raw, &choices)
					for _, choice := range choices {
						reasoning.WriteString(choice.Delta.Reasoning)
					}
				}
				if reasoning.String() != result.ThinkingText {
					t.Fatal("public reasoning was not sent as standard reasoning_content")
				}
				assertCompletedChatStream(t, writer.Body.String(), want, "stop")
			})
		}
	}
}

func TestReasoningRecapIsNotACompletedConversation(t *testing.T) {
	snapshot := handoffTestSnapshot(t, false)
	mapping := snapshot["mapping"].(map[string]interface{})
	answer := mapping["current-answer"].(map[string]interface{})["message"].(map[string]interface{})
	answer["content"] = map[string]interface{}{"content_type": "reasoning_recap", "content": "Thought for 3m"}
	_, complete, err := completedConversationSSE("conv-handoff-test", handoffTestUserID, snapshot)
	if err != nil || complete {
		t.Fatalf("recap prematurely completed turn: completed=%t err=%v", complete, err)
	}
}

func TestPrivateThoughtsDoNotBecomeArtifacts(t *testing.T) {
	for _, payload := range []string{
		`{"message":{"id":"thought","author":{"role":"assistant"},"content":{"content_type":"thoughts","thoughts":[{"summary":"Checking sources","content":"sandbox:/mnt/data/PRIVATE_SECRET.txt","chunks":["sediment://file-private"]}]}}}`,
		`{"p":"/message/content/thoughts/0/content","o":"append","v":"sandbox:/mnt/data/PRIVATE_SECRET.txt"}`,
		`{"v":[{"p":"/message/content/thoughts/0/chunks","o":"append","v":[{"asset_pointer":"sediment://file-private","gen_id":"private-gen"}]}]}`,
	} {
		var raw map[string]interface{}
		if err := json.Unmarshal([]byte(payload), &raw); err != nil {
			t.Fatal(err)
		}
		accumulator := newArtifactAccumulator()
		if events := accumulator.ObserveRaw(raw, "conv"); len(events) != 0 ||
			len(accumulator.Signals) != 0 || len(accumulator.SandboxArtifacts) != 0 ||
			len(accumulator.ImageFileIDs) != 0 {
			t.Fatal("private thought content produced a public artifact")
		}
	}
}

func TestHandoffPublicProgressIsScopedAndDeduplicated(t *testing.T) {
	for _, wrongTurn := range []bool{false, true} {
		t.Run(fmt.Sprintf("wrong-turn=%t", wrongTurn), func(t *testing.T) {
			final := handoffTestSnapshot(t, false)
			pending := handoffTestSnapshot(t, false)
			thought := map[string]interface{}{
				"parent": handoffTestUserID,
				"message": map[string]interface{}{
					"id": "thought-progress", "author": map[string]interface{}{"role": "assistant"},
					"content": map[string]interface{}{
						"content_type": "thoughts",
						"thoughts": []interface{}{map[string]interface{}{
							"summary": "Checking sources", "content": "PRIVATE_DETAIL", "chunks": []string{"PRIVATE_CHUNK"},
						}},
					},
					"end_turn": false,
				},
			}
			pending["mapping"].(map[string]interface{})["thought-progress"] = thought
			pending["current_node"] = "thought-progress"
			if wrongTurn {
				pending["mapping"].(map[string]interface{})[handoffTestUserID].(map[string]interface{})["message"].(map[string]interface{})["id"] = "different-user-turn"
			} else {
				mapping := final["mapping"].(map[string]interface{})
				mapping["thought-progress"] = thought
				mapping["current-answer"].(map[string]interface{})["parent"] = "thought-progress"
			}
			writer := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(writer)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
			client := &handoffTestClient{t: t, snapshot: final, pendingSnapshot: pending, pendingReads: 1}
			client.beforeRead = func() {
				if client.conversationReads == 2 {
					visible := strings.Contains(writer.Body.String(), "Checking sources")
					if visible == wrongTurn {
						t.Fatal("pending public activity was missing or leaked from another turn")
					}
				}
			}
			request := chatGPTRequestForTest()
			if err := json.Unmarshal([]byte(`{"messages":[{"id":"00000000-0000-4000-8000-000000000001","author":{"role":"user"}}]}`), &request); err != nil {
				t.Fatal(err)
			}
			body := "data: {\"conversation_id\":\"conv-handoff-test\"}\n\ndata: {\"type\":\"stream_handoff\",\"options\":[{\"type\":\"subscribe_ws_topic\",\"topic_id\":\"test-topic\"}]}\n\ndata: [DONE]\n\n"
			result := HandlerDetailedWithOptions(c, &http.Response{Body: io.NopCloser(strings.NewReader(body))}, client, nil, "test", request, true, "gpt-6-pro", HandlerDetailedOptions{})
			want := 1
			if wrongTurn {
				want = 0
			}
			if result.Err != nil || strings.Count(result.ThinkingText, "Checking sources") != want ||
				strings.Contains(writer.Body.String(), "PRIVATE_") || result.Text != "A synthetic title" {
				t.Fatalf("bad recovered response: text=%q reasoning=%q err=%v", result.Text, result.ThinkingText, result.Err)
			}
		})
	}
}
