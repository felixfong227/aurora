package handler

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"aurora/httpclient/bogdanfinn"
	"aurora/internal/accounts"
	"aurora/internal/chatgpt"
	"aurora/internal/config"
	"aurora/internal/sseparser"
	chattypes "aurora/typings/chatgpt"
	official "aurora/typings/official"

	fhttp "github.com/bogdanfinn/fhttp"
	fhttptest "github.com/bogdanfinn/fhttp/httptest"
	"github.com/bogdanfinn/websocket"
	"github.com/gin-gonic/gin"
)

type continuityFixture struct {
	mu       sync.Mutex
	requests []chattypes.ChatGPTRequest
	handler  *ChatHandler
	cfg      *config.Config
	account  *accounts.Account
	pings    chan http.Header
}

func newContinuityFixture(t *testing.T, tool, interrupted bool) *continuityFixture {
	t.Helper()
	f := &continuityFixture{pings: make(chan http.Header, 16)}
	oldBase, oldTransport := chatgpt.BaseURL, http.DefaultTransport
	http.DefaultTransport = toolStreamOfflineTransport{} // Blocks tokenizer downloads.
	wsServer := fhttptest.NewServer(fhttp.HandlerFunc(func(w fhttp.ResponseWriter, r *fhttp.Request) {
		upgrader := websocket.Upgrader{CheckOrigin: func(*fhttp.Request) bool { return true }}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	t.Cleanup(wsServer.Close)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/sentinel/chat-requirements/prepare":
			fmt.Fprint(w, `{"prepare_token":"fake-prepare"}`)
		case "/sentinel/chat-requirements/finalize":
			fmt.Fprint(w, `{"token":"fake-sentinel"}`)
		case "/sentinel/ping":
			f.pings <- r.Header.Clone()
			fmt.Fprint(w, `{}`)
		case "/conversation/init":
			fmt.Fprint(w, `{}`)
		case "/celsius/ws/user":
			fmt.Fprintf(w, `{"websocket_url":%q}`, "ws"+strings.TrimPrefix(wsServer.URL, "http"))
		case "/f/conversation/prepare":
			fmt.Fprint(w, `{"conduit_token":"fake-conduit"}`)
		case "/f/conversation":
			var request chattypes.ChatGPTRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Error(err)
				return
			}
			f.mu.Lock()
			f.requests = append(f.requests, request)
			n := len(f.requests)
			f.mu.Unlock()
			conversation := request.ConversationID
			if conversation == "" {
				conversation = fmt.Sprintf("upstream-%d", n)
			}
			text := fmt.Sprintf("answer-%d", n)
			if tool && n == 1 {
				text = `<tool_call>{"name":"lookup","arguments":{"q":"fixture"}}</tool_call>`
			}
			w.Header().Set("Content-Type", "text/event-stream")
			fmt.Fprintf(w, `data: {"conversation_id":%q,"message":{"id":%q,"author":{"role":"assistant"},"recipient":"all","content":{"content_type":"text","parts":[%q]},"end_turn":%t,"metadata":{"message_type":"next"}}}`+"\n\n",
				conversation, fmt.Sprintf("parent-%d", n), text, !interrupted)
			if !interrupted {
				fmt.Fprint(w, "data: [DONE]\n\n")
			}
		default:
			t.Errorf("unexpected upstream endpoint: %s", r.URL.Path)
			http.Error(w, "not a fixture endpoint", 404)
		}
	}))
	chatgpt.BaseURL = server.URL
	client := bogdanfinn.NewStdClient()
	client.Client.SetFollowRedirect(false)
	client.ReqBefore = func(r *fhttp.Request) error {
		if r.URL.Scheme+"://"+r.URL.Host != server.URL {
			t.Errorf("blocked non-fixture request: %s", r.URL)
			return fmt.Errorf("only loopback fixture requests are permitted")
		}
		return nil
	}
	client.SetCookies("https://chatgpt.com", []*http.Cookie{{Name: "cf_clearance", Value: "fake-clearance"}})
	f.account = relayTestAccount("synthetic-upstream-user", "synthetic-token")
	f.account.Client = client
	f.cfg = &config.Config{
		Authorization: "synthetic-gateway-key", ConversationStateDir: filepath.Join(t.TempDir(), "state"),
		ToolCallingEnabled: true, StreamMode: true, MaxContinueCount: 1,
	}
	f.handler = NewChatHandler(accounts.NewPool([]*accounts.Account{f.account}), f.cfg)
	t.Cleanup(func() {
		server.Close()
		client.Client.CloseIdleConnections()
		chatgpt.BaseURL, http.DefaultTransport = oldBase, oldTransport
	})
	return f
}

func (f *continuityFixture) call(t *testing.T, user string, request official.APIRequest, status int) official.APIMessage {
	t.Helper()
	body, _ := json.Marshal(request)
	writer := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(writer)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	c.Request.Header.Set("Authorization", "Bearer "+f.cfg.Authorization)
	c.Request.Header.Set("X-Aurora-User-Id", user)
	c.Request.Header.Set("X-Aurora-Conversation-Id", "same-client-conversation")
	f.handler.Nightmare(c)
	if writer.Code != status {
		t.Fatalf("status %d, want %d: %s", writer.Code, status, writer.Body.String())
	}
	if status != 200 {
		return official.APIMessage{}
	}
	if !request.Stream {
		var response struct {
			Choices []struct{ Message official.APIMessage }
		}
		if err := json.Unmarshal(writer.Body.Bytes(), &response); err != nil || len(response.Choices) != 1 {
			t.Fatalf("invalid JSON completion: %s", writer.Body.String())
		}
		return response.Choices[0].Message
	}
	output := official.NewTextMessage("assistant", "")
	done := 0
	for _, data := range sseparser.DataPayloads(writer.Body.String()) {
		if data == "[DONE]" {
			done++
			continue
		}
		var chunk official.ChatCompletionChunk
		if err := json.Unmarshal([]byte(data), &chunk); err != nil {
			t.Fatal(err)
		}
		for _, choice := range chunk.Choices {
			output.Content.TextValue += choice.Delta.Content
			for _, call := range choice.Delta.ToolCalls {
				ref := official.ToolCallRef{ID: call.ID, Type: call.Type, Index: call.Index}
				ref.Function.Name, ref.Function.Arguments = call.Function.Name, call.Function.Arguments
				output.ToolCalls = append(output.ToolCalls, ref)
			}
		}
	}
	if done != 1 {
		t.Fatalf("missing terminal SSE: %s", writer.Body.String())
	}
	return output
}

func (f *continuityFixture) posted() []chattypes.ChatGPTRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]chattypes.ChatGPTRequest(nil), f.requests...)
}

func TestContinuityFirstTurnPingSnapshot(t *testing.T) {
	f := newContinuityFixture(t, false, false)
	f.call(t, "alice", official.APIRequest{Messages: []official.APIMessage{official.NewTextMessage("user", "hello")}}, 200)
	select {
	case headers := <-f.pings:
		if got := headers.Get("Referer"); got != "https://chatgpt.com/c/upstream-1" {
			t.Fatalf("ping used stale first-turn snapshot: referer=%q", got)
		}
		var extra struct {
			ConversationID string `json:"conversation_id"`
			LastMessageID  string `json:"last_message_id"`
		}
		data, err := base64.StdEncoding.DecodeString(headers.Get("Openai-Sentinel-Extra-Data"))
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(data, &extra); err != nil {
			t.Fatal(err)
		}
		if extra.ConversationID != "WEB:upstream-1" || extra.LastMessageID != "parent-1" {
			t.Fatalf("ping used wrong turn result: %+v", extra)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("first-turn ping not received")
	}
}

type continuityObservingWriter struct {
	*httptest.ResponseRecorder
	onWrite func(string)
}

func (w *continuityObservingWriter) Write(b []byte) (int, error) {
	w.onWrite(string(b))
	return w.ResponseRecorder.Write(b)
}

func (w *continuityObservingWriter) WriteString(s string) (int, error) {
	return w.Write([]byte(s))
}

func TestContinuityStreamCommitsBeforeTerminal(t *testing.T) {
	for _, failCommit := range []bool{false, true} {
		t.Run(fmt.Sprintf("failCommit=%t", failCommit), func(t *testing.T) {
			f := newContinuityFixture(t, false, false)
			stateDir := f.cfg.ConversationStateDir
			readInflight := func() bool {
				t.Helper()
				files, err := filepath.Glob(filepath.Join(stateDir, "*.json"))
				if err != nil || len(files) != 1 {
					t.Fatalf("checkpoint files=%v err=%v", files, err)
				}
				data, err := os.ReadFile(files[0])
				if err != nil {
					t.Fatal(err)
				}
				var state struct{ Inflight bool }
				if err := json.Unmarshal(data, &state); err != nil {
					t.Fatal(err)
				}
				return state.Inflight
			}
			var events []string
			writer := &continuityObservingWriter{ResponseRecorder: httptest.NewRecorder()}
			writer.onWrite = func(s string) {
				for _, data := range sseparser.DataPayloads(s) {
					if data == "[DONE]" {
						events = append(events, "done")
						continue
					}
					var chunk official.ChatCompletionChunk
					if err := json.Unmarshal([]byte(data), &chunk); err != nil {
						t.Fatal(err)
					}
					if len(chunk.Choices) == 0 {
						events = append(events, "usage")
					}
					for _, choice := range chunk.Choices {
						if choice.Delta.Content != "" {
							events = append(events, "text")
							if !readInflight() {
								t.Fatal("text was buffered until after commit")
							}
							if failCommit {
								// Keep the durable Start record, but make Commit's
								// CreateTemp fail without relying on permission bits.
								if err := os.Rename(stateDir, stateDir+"-started"); err != nil {
									t.Fatal(err)
								}
								if err := os.WriteFile(stateDir, nil, 0600); err != nil {
									t.Fatal(err)
								}
								stateDir += "-started"
							}
						}
						if choice.FinishReason != nil {
							events = append(events, "finish")
							if readInflight() {
								t.Fatal("terminal success sent before durable commit")
							}
						}
					}
				}
			}
			c, _ := gin.CreateTestContext(writer)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(
				`{"stream":true,"stream_options":{"include_usage":true},"messages":[{"role":"user","content":"hello"}]}`))
			c.Request.Header.Set("Authorization", "Bearer "+f.cfg.Authorization)
			c.Request.Header.Set("X-Aurora-User-Id", "alice")
			c.Request.Header.Set("X-Aurora-Conversation-Id", "chat")
			f.handler.Nightmare(c)
			want := "text,finish,usage,done"
			if failCommit {
				want = "text"
				if !strings.Contains(writer.Body.String(), `"code":"upstream_error"`) {
					t.Fatalf("store failure was not propagated: %s", writer.Body.String())
				}
			}
			if got := strings.Join(events, ","); got != want || readInflight() != failCommit {
				t.Fatalf("events=%s, want %s; output=%s", got, want, writer.Body.String())
			}
		})
	}
}

func TestContinuityHandlerTextIsolationRestartAndBranch(t *testing.T) {
	f := newContinuityFixture(t, false, false)
	first := official.APIRequest{Model: "gpt-6-pro", Messages: []official.APIMessage{
		official.NewTextMessage("system", "fixed instructions"),
		official.NewTextMessage("user", "identical prompt"),
	}}
	alice := f.call(t, "alice", first, 200)
	f.call(t, "bob", first, 200)
	// Restart the handler/store and renew the same synthetic account identity.
	renewed := relayTestAccount("synthetic-upstream-user", "renewed-token")
	renewed.Client = f.account.Client
	f.handler = NewChatHandler(accounts.NewPool([]*accounts.Account{renewed}), f.cfg)
	next := first
	next.Model = "gpt-6"
	next.Messages = append(append([]official.APIMessage(nil), first.Messages...), alice, official.NewTextMessage("user", "only this suffix"))
	f.call(t, "alice", next, 200)
	posts := f.posted()
	if len(posts) != 3 || posts[0].ConversationID != "" || posts[1].ConversationID != "" ||
		posts[2].ConversationID != "upstream-1" || posts[2].ParentMessageID != "parent-1" ||
		len(posts[2].Messages) != 1 || posts[2].Messages[0].Content.Parts[0] != "only this suffix" ||
		posts[2].Model != "gpt-6" {
		t.Fatalf("wrong continuity requests: %+v", posts)
	}
	next.Messages[len(next.Messages)-1] = official.NewTextMessage("user", "branch from first answer")
	f.call(t, "alice", next, 200)
	if last := f.posted()[3]; last.ConversationID != "upstream-1" || last.ParentMessageID != "parent-1" {
		t.Fatal("branch selected the latest parent instead of the proven parent")
	}
	f.call(t, "alice", next, 409) // Same request must not create another generation.
	f.call(t, "alice", first, 409)

	other := relayTestAccount("different-upstream-user", "other-token")
	other.Client = f.account.Client
	f.handler = NewChatHandler(accounts.NewPool([]*accounts.Account{other}), f.cfg)
	next.Messages[len(next.Messages)-1] = official.NewTextMessage("user", "another branch")
	f.call(t, "alice", next, 409)
	if len(f.posted()) != 4 {
		t.Fatal("missing bound account silently selected another upstream account")
	}
}

func TestContinuityHandlerToolFollowup(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprintf("stream=%t", stream), func(t *testing.T) {
			f := newContinuityFixture(t, true, false)
			req := official.APIRequest{Model: "gpt-6-pro", Stream: stream,
				Messages: []official.APIMessage{official.NewTextMessage("user", "lookup fixture")},
				Tools:    []official.Tool{{Type: "function", Function: official.ToolFunction{Name: "lookup"}}},
			}
			output := f.call(t, "alice", req, 200)
			if output.Text() != "" || len(output.ToolCalls) != 1 {
				t.Fatalf("tool output was not canonical: %+v", output)
			}
			result := official.NewTextMessage("tool", "synthetic tool result")
			result.ToolCallID = output.ToolCalls[0].ID
			req.Messages = append(req.Messages, output, result)
			answer := f.call(t, "alice", req, 200)
			posts := f.posted()
			if len(posts) != 2 || posts[1].ConversationID != "upstream-1" || posts[1].ParentMessageID != "parent-1" {
				t.Fatal("tool followup created another conversation")
			}
			b, _ := json.Marshal(posts[1].Messages)
			if strings.Contains(string(b), "lookup fixture") || strings.Contains(string(b), "<tool_calling_protocol>") ||
				!strings.Contains(string(b), "synthetic tool result") || !strings.Contains(string(b), "lookup") {
				t.Fatalf("tool request repeated history or lost result: %s", b)
			}
			req.Messages = append(req.Messages, answer, official.NewTextMessage("user", "next task"))
			f.call(t, "alice", req, 200)
			last := f.posted()[2]
			b, _ = json.Marshal(last.Messages)
			if last.ConversationID != "upstream-1" || last.ParentMessageID != "parent-2" ||
				strings.Contains(string(b), "synthetic tool result") || !strings.Contains(string(b), "next task") {
				t.Fatalf("turn after tool result lost continuity: %s", b)
			}
		})
	}
}

func TestContinuityHandlerInterruptedTurnBlocksRetry(t *testing.T) {
	f := newContinuityFixture(t, false, true)
	req := official.APIRequest{Model: "gpt-6-pro", Messages: []official.APIMessage{official.NewTextMessage("user", "hello")}}
	f.call(t, "alice", req, http.StatusBadGateway)
	f.handler = NewChatHandler(accounts.NewPool([]*accounts.Account{f.account}), f.cfg)
	f.call(t, "alice", req, http.StatusConflict)
	if len(f.posted()) != 1 {
		t.Fatal("unknown outcome retried upstream")
	}
}

func TestContinuityHeadersFailClosed(t *testing.T) {
	req := official.APIRequest{Messages: []official.APIMessage{official.NewTextMessage("user", "hello")}}
	h := NewChatHandler(accounts.NewPool(nil), &config.Config{Authorization: "gateway"})
	for _, headers := range []map[string]string{
		{"X-Aurora-User-Id": "user"},
		{"X-Aurora-Conversation-Id": "conversation"},
		{"X-Aurora-User-Id": "", "X-Aurora-Conversation-Id": "conversation"},
		{"X-Aurora-User-Id": "user", "X-Aurora-Conversation-Id": "conversation"},
		{"X-Aurora-User-Id": "{{LIBRECHAT_USER_ID}}", "X-Aurora-Conversation-Id": "conversation"},
	} {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = httptest.NewRequest("POST", "/", nil)
		c.Request.Header.Set("Authorization", "Bearer gateway")
		for k, v := range headers {
			c.Request.Header.Set(k, v)
		}
		if _, _, err := h.prepareConversation(c, req); err == nil {
			t.Fatal("missing metadata/state configuration fell back to stateless")
		}
	}
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("POST", "/", nil)
	if lease, account, err := h.prepareConversation(c, req); lease != nil || account != nil || err != nil {
		t.Fatal("non-opted-in client did not retain stateless behavior")
	}
	h = NewChatHandler(accounts.NewPool(nil), &config.Config{
		Authorization: "gateway", ConversationStateDir: filepath.Join(t.TempDir(), "state"),
	})
	c.Request.Header.Set("X-Aurora-User-Id", "user")
	c.Request.Header.Set("X-Aurora-Conversation-Id", "conversation")
	for _, credential := range []string{"", "Bearer external-token", "Bearer gateway external-token"} {
		c.Request.Header.Set("Authorization", credential)
		if _, _, err := h.prepareConversation(c, req); err == nil || !strings.Contains(err.Error(), "gateway credential") {
			t.Fatal("continuity accepted an unauthenticated/external-token scope")
		}
	}
	c.Request.Header.Set("Authorization", "Bearer gateway")
	c.Request.Header.Add("X-Aurora-User-Id", "other-user")
	if _, _, err := h.prepareConversation(c, req); err == nil || !strings.Contains(err.Error(), "one nonempty") {
		t.Fatal("duplicate identity headers were accepted")
	}
}
