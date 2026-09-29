package handler

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"aurora/httpclient/bogdanfinn"
	"aurora/internal/accounts"
	"aurora/internal/chatgpt"
	"aurora/internal/config"
	"aurora/internal/sseparser"
	officialtypes "aurora/typings/official"

	fhttp "github.com/bogdanfinn/fhttp"
	"github.com/gin-gonic/gin"
)

// Token counting may download a tokenizer on a cold cache. This test does not
// need token counts, and must remain offline even without that cache.
type toolStreamOfflineTransport struct{}

func (toolStreamOfflineTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	return nil, fmt.Errorf("offline test blocked standard HTTP request to %s", r.URL)
}

func TestNightmareToolCallingStream(t *testing.T) {
	// BaseURL and DefaultTransport are process globals: do not parallelize.
	baseURL, transport := chatgpt.BaseURL, http.DefaultTransport
	http.DefaultTransport = toolStreamOfflineTransport{}
	t.Cleanup(func() {
		chatgpt.BaseURL = baseURL
		http.DefaultTransport = transport
	})

	for _, tc := range []struct {
		name   string
		text   string
		tool   bool
		finish string
	}{
		{"image tool", `<tool_call>{"name":"image_gen_oai","arguments":{"prompt":"synthetic image"}}</tool_call>`, true, "tool_calls"},
		{"plain text fallback", "Synthetic answer.", false, "stop"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := make(map[string]int)
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls[r.URL.Path]++
				if r.Method != http.MethodPost {
					t.Errorf("unexpected upstream method: %s", r.Method)
				}
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/sentinel/chat-requirements/prepare":
					fmt.Fprint(w, `{"prepare_token":"fake-prepare"}`)
				case "/sentinel/chat-requirements/finalize":
					fmt.Fprint(w, `{"token":"fake-sentinel"}`)
				case "/conversation/init":
					fmt.Fprint(w, `{}`)
				case "/f/conversation/prepare":
					fmt.Fprint(w, `{"conduit_token":"fake-conduit"}`)
				case "/f/conversation":
					w.Header().Set("Content-Type", "text/event-stream")
					fmt.Fprintf(w, `data: {"conversation_id":"conv-fixture","message":{"id":"msg-fixture","author":{"role":"assistant"},"recipient":"all","content":{"content_type":"text","parts":[%q]},"end_turn":true,"metadata":{"message_type":"next","finish_details":{"type":"stop"}}}}`+"\n\ndata: [DONE]\n\n", tc.text)
				default:
					// In particular, never implement a model or image endpoint.
					t.Errorf("unexpected upstream endpoint: %s", r.URL.Path)
					http.Error(w, "unexpected endpoint", http.StatusNotFound)
				}
			}))
			defer upstream.Close()
			chatgpt.BaseURL = upstream.URL

			client := bogdanfinn.NewStdClient()
			client.Client.SetFollowRedirect(false)
			defer client.Client.CloseIdleConnections()
			client.ReqBefore = func(r *fhttp.Request) error {
				if r.URL.Scheme+"://"+r.URL.Host != upstream.URL {
					t.Errorf("blocked non-fixture request: %s", r.URL)
					return fmt.Errorf("only the loopback fixture is allowed")
				}
				return nil
			}
			// Skip hard-coded provider cookie bootstrap without making a request.
			client.SetCookies("https://chatgpt.com", []*http.Cookie{{Name: "cf_clearance", Value: "fake-clearance"}})
			account := accounts.NewAccount("fixture", accounts.TypeFree, "fake-access-token")
			account.Status = accounts.StatusActive
			account.Client = client
			h := NewChatHandler(accounts.NewPool([]*accounts.Account{account}), &config.Config{ToolCallingEnabled: true})
			writer := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(writer)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{
				"model":"gpt-6-pro","stream":true,
				"messages":[{"role":"user","content":"Create a synthetic image."}],
				"tools":[{"type":"function","function":{"name":"image_gen_oai","parameters":{"type":"object","properties":{"prompt":{"type":"string"}}}}}]
			}`))
			c.Request.Header.Set("Content-Type", "application/json")

			h.Nightmare(c)

			if writer.Code != http.StatusOK || !strings.HasPrefix(writer.Header().Get("Content-Type"), "text/event-stream") {
				t.Fatalf("Nightmare returned %d %s instead of SSE: %s", writer.Code, writer.Header().Get("Content-Type"), writer.Body.String())
			}
			var text string
			var tools []officialtypes.ToolCallDelta
			stops, done := 0, 0
			payloads := sseparser.DataPayloads(writer.Body.String())
			for _, payload := range payloads {
				if payload == "[DONE]" {
					done++
					continue
				}
				var chunk officialtypes.ChatCompletionChunk
				if err := json.Unmarshal([]byte(payload), &chunk); err != nil {
					t.Fatal(err)
				}
				if chunk.Object != "chat.completion.chunk" || len(chunk.Choices) != 1 {
					t.Fatalf("invalid completion chunk: %s", payload)
				}
				choice := chunk.Choices[0]
				text += choice.Delta.Content
				tools = append(tools, choice.Delta.ToolCalls...)
				if choice.FinishReason != nil {
					stops++
					if choice.FinishReason != tc.finish {
						t.Errorf("finish reason = %v, want %s", choice.FinishReason, tc.finish)
					}
				}
			}
			if done != 1 || stops != 1 || len(payloads) == 0 || payloads[len(payloads)-1] != "[DONE]" {
				t.Fatalf("invalid stream termination: %s", writer.Body.String())
			}
			if tc.tool {
				if len(tools) != 1 {
					t.Fatalf("tool deltas = %v, want one image tool", tools)
				}
				call := tools[0]
				if call.Index != 0 || call.ID == "" || call.Type != "function" || call.Function.Name != "image_gen_oai" || call.Function.Arguments != `{"prompt":"synthetic image"}` {
					t.Fatalf("invalid image tool delta: %+v", call)
				}
			} else if text != tc.text || len(tools) != 0 {
				t.Fatalf("plain text fallback = %q, tools = %v", text, tools)
			}
			for path, want := range map[string]int{
				"/sentinel/chat-requirements/prepare": 1, "/sentinel/chat-requirements/finalize": 1,
				"/conversation/init": 1, "/f/conversation/prepare": 3, "/f/conversation": 1,
			} {
				if calls[path] != want {
					t.Errorf("upstream %s called %d times, want %d", path, calls[path], want)
				}
			}
		})
	}
}
