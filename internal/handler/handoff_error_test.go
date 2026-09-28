package handler

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestWriteResponsesHandlerError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Header("Content-Type", "text/event-stream")
	c.Writer.WriteString("event: response.created\ndata: {}\n\n")
	c.Writer.Flush()
	w.Body.Reset()

	writeResponsesHandlerError(c, errors.New("incomplete handoff"))
	if !c.IsAborted() || w.Code != http.StatusOK || w.Header().Get("Content-Type") != "text/event-stream" {
		t.Fatalf("failure did not terminate the Responses stream: %d %v", w.Code, w.Header())
	}
	body := w.Body.String()
	const prefix = "event: response.failed\ndata: "
	if !strings.HasPrefix(body, prefix) || !strings.HasSuffix(body, "\n\n") {
		t.Fatalf("invalid Responses failure event: %q", body)
	}
	if strings.Contains(body, "response.completed") || strings.Contains(body, "[DONE]") {
		t.Fatalf("failure emitted a successful completion: %s", body)
	}
	var event struct {
		Type     string `json:"type"`
		Response struct {
			Error struct {
				Message string `json:"message"`
				Type    string `json:"type"`
			} `json:"error"`
		} `json:"response"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSuffix(strings.TrimPrefix(body, prefix), "\n\n")), &event); err != nil {
		t.Fatal(err)
	}
	if event.Type != "response.failed" || event.Response.Error.Message != "incomplete handoff" || event.Response.Error.Type != "server_error" {
		t.Fatalf("unexpected failure event: %+v", event)
	}
}
