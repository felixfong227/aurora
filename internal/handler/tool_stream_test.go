package handler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"aurora/internal/sseparser"
	officialtypes "aurora/typings/official"

	"github.com/gin-gonic/gin"
)

func TestToolCallingResponseHonorsStream(t *testing.T) {
	gin.SetMode(gin.TestMode)
	calls := []officialtypes.ToolCall{
		{ID: "call-image", Type: "function", Function: officialtypes.ToolCallFunc{Name: "image_gen_oai", Arguments: `{"prompt":"synthetic image"}`}},
		{ID: "call-other", Type: "function", Function: officialtypes.ToolCallFunc{Name: "other_tool", Arguments: `{"value":2}`}},
	}
	for _, withCalls := range []bool{false, true} {
		for _, stream := range []bool{false, true} {
			result := toolCallingResult{Text: "Fixture answer", ConversationID: "conv-fixture", Sentinel: []map[string]interface{}{{"event": "fixture"}}}
			if withCalls {
				result.ToolCalls = calls
			}
			writer := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(writer)
			request := &officialtypes.APIRequest{Stream: stream, StreamOptions: &officialtypes.StreamOptions{IncludeUsage: true}}
			writeToolCallingResult(c, request, result, "gpt-6-pro", 7)
			if !stream {
				var response officialtypes.ChatCompletion
				if err := json.Unmarshal(writer.Body.Bytes(), &response); err != nil {
					t.Fatal(err)
				}
				if writer.Code != http.StatusOK || len(response.Choices) != 1 {
					t.Fatal("invalid nonstream completion")
				}
				continue
			}
			if !strings.HasPrefix(writer.Header().Get("Content-Type"), "text/event-stream") {
				t.Fatalf("stream=%t tools=%t returned %s, not SSE", stream, withCalls, writer.Header().Get("Content-Type"))
			}
			var text string
			var toolDeltas []officialtypes.ToolCallDelta
			stops, done, usage, sentinels := 0, 0, 0, 0
			for _, payload := range sseparser.DataPayloads(writer.Body.String()) {
				if payload == "[DONE]" {
					done++
					continue
				}
				var chunk officialtypes.ChatCompletionChunk
				if err := json.Unmarshal([]byte(payload), &chunk); err != nil {
					t.Fatal(err)
				}
				if chunk.Object != "chat.completion.chunk" {
					t.Fatal("nonstream object inside stream")
				}
				if chunk.Usage != nil {
					usage++
					if len(chunk.Choices) != 0 || chunk.Usage.PromptTokens != 7 {
						t.Fatal("invalid stream usage")
					}
				}
				if chunk.Sentinel != nil {
					sentinels++
				}
				for _, choice := range chunk.Choices {
					text += choice.Delta.Content
					toolDeltas = append(toolDeltas, choice.Delta.ToolCalls...)
					if choice.FinishReason != nil {
						stops++
						want := "stop"
						if withCalls {
							want = "tool_calls"
						}
						if choice.FinishReason != want {
							t.Fatalf("finish reason = %v, want %s", choice.FinishReason, want)
						}
					}
				}
			}
			if text != result.Text || stops != 1 || done != 1 || usage != 1 || sentinels != 1 {
				t.Fatal("stream lost content, metadata, usage or termination")
			}
			if len(toolDeltas) != len(result.ToolCalls) {
				t.Fatal("stream lost tool calls")
			}
			for index, call := range toolDeltas {
				if call.Index != index || call.ID != calls[index].ID || call.Function.Name != calls[index].Function.Name || call.Function.Arguments != calls[index].Function.Arguments {
					t.Fatal("tool call changed during stream serialization")
				}
			}
		}
	}
}
