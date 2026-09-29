package httpstream

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
)

func TestWriteChatCompletionError(t *testing.T) {
	for _, mode := range []string{"json", "staged_sse", "started_sse"} {
		t.Run(mode, func(t *testing.T) {
			c, w := setupTestContext()
			if mode != "json" {
				WriteSSEHeader(c)
			}
			if mode == "started_sse" {
				WriteSSEEvent(c, "", map[string]string{"delta": "partial"})
				w.Body.Reset()
			}
			WriteChatCompletionError(c, errors.New("incomplete handoff"))
			if !c.IsAborted() {
				t.Fatal("failure must abort the request")
			}
			body := w.Body.String()
			if strings.Contains(body, "[DONE]") || strings.Contains(body, "finish_reason") {
				t.Fatalf("failure emitted a successful completion: %s", body)
			}
			if mode == "started_sse" {
				if w.Code != http.StatusOK || w.Header().Get("Content-Type") != "text/event-stream" {
					t.Fatalf("changed committed SSE response: %d %v", w.Code, w.Header())
				}
				if !strings.HasPrefix(body, "data: ") || !strings.HasSuffix(body, "\n\n") {
					t.Fatalf("invalid SSE error: %q", body)
				}
				body = strings.TrimSuffix(strings.TrimPrefix(body, "data: "), "\n\n")
			} else if w.Code != http.StatusBadGateway || !strings.HasPrefix(w.Header().Get("Content-Type"), "application/json") {
				t.Fatalf("expected JSON 502 before commit: %d %v", w.Code, w.Header())
			}
			var payload struct {
				Error struct {
					Message string `json:"message"`
					Type    string `json:"type"`
					Code    string `json:"code"`
				} `json:"error"`
			}
			if err := json.Unmarshal([]byte(body), &payload); err != nil {
				t.Fatal(err)
			}
			if payload.Error.Message != "incomplete handoff" || payload.Error.Type != "server_error" || payload.Error.Code != "upstream_error" {
				t.Fatalf("unexpected error envelope: %+v", payload)
			}
		})
	}
}
