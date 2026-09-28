package chatgpt

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

	"aurora/conversion/response/chatgpt"
	"aurora/httpclient"
	"aurora/internal/accounts"
	"aurora/internal/httpstream"
	"aurora/internal/sseparser"
	"aurora/typings"
	chatgpt_types "aurora/typings/chatgpt"
	official_types "aurora/typings/official"

	"github.com/bogdanfinn/websocket"
)

type conversationPatchState struct {
	response chatgpt_types.ChatGPTResponse
	channel  string
}

type conversationStreamEvent struct {
	response       chatgpt_types.ChatGPTResponse
	chunk          *official_types.ChatCompletionChunk
	text           string
	role           string
	conversationID string
	messageID      string
	channel        string
	finishReason   string
	isStop         bool
}

func parseConversationEvent(line string, state *sseparser.PatchState, model string) (conversationStreamEvent, bool) {
	var raw map[string]interface{}
	if err := json.Unmarshal([]byte(line), &raw); err != nil {
		return conversationStreamEvent{}, false
	}

	if chunk, ok := sseparser.ChunkFromRaw(raw, model); ok {
		event := conversationStreamEvent{
			chunk:          &chunk,
			text:           sseparser.ChunkContent(chunk),
			role:           sseparser.ChunkRole(chunk),
			conversationID: chunk.ConversationID,
			channel:        sseparser.ChannelFromValue(raw),
			finishReason:   sseparser.ChunkFinishReason(chunk),
		}
		event.isStop = event.finishReason != ""
		return event, true
	}

	var direct chatgpt_types.ChatGPTResponse
	if err := json.Unmarshal([]byte(line), &direct); err == nil && sseparser.IsUsableConversationResponse(direct) {
		channel := sseparser.ChannelFromValue(raw)
		state.Channel = firstNonEmpty(channel, state.Channel)
		sseparser.RecordContentReferences(state, direct.Message.Metadata.ContentReferences)
		return conversationStreamEvent{response: direct, messageID: direct.Message.ID, channel: state.Channel}, true
	}

	if response, ok := sseparser.ResponseFromValue(raw["v"]); ok {
		if state.Response.Message.ID != "" && response.Message.ID != "" && response.Message.ID != state.Response.Message.ID {
			sseparser.ResetMessage(state)
		}
		state.Response = response
		sseparser.RecordContentReferences(state, response.Message.Metadata.ContentReferences)
		if channel := sseparser.ChannelFromValue(raw["v"]); channel != "" {
			state.Channel = channel
		}
		return conversationStreamEvent{response: state.Response, messageID: state.Response.Message.ID, channel: state.Channel}, true
	}
	if text, ok := raw["v"].(string); ok && raw["p"] == nil {
		sseparser.EnsurePatchDefaults(state)
		current, _ := state.Response.Message.Content.Parts[0].(string)
		state.Response.Message.Content.Parts[0] = current + text
		return conversationStreamEvent{response: state.Response, messageID: state.Response.Message.ID, channel: state.Channel}, true
	}

	// 裸补丁数组: {"v":[...]} 或省略 p 的变体 {"o":"patch","v":[...]}
	if batch, ok := raw["v"].([]interface{}); ok && raw["p"] == nil {
		applied := false
		sseparser.EnsurePatchDefaults(state)
		for _, item := range batch {
			op, ok := item.(map[string]interface{})
			if !ok {
				continue
			}
			subPath, _ := op["p"].(string)
			subOp, _ := op["o"].(string)
			if sseparser.ApplyPatch(state, subPath, subOp, op["v"]) {
				applied = true
			}
		}
		if applied {
			return conversationStreamEvent{response: state.Response, messageID: state.Response.Message.ID, channel: state.Channel}, true
		}
	}

	if patchPath, ok := raw["p"].(string); ok {
		patchOperation, _ := raw["o"].(string)
		if patchPath == "" && patchOperation == "patch" {
			if batch, ok := raw["v"].([]interface{}); ok {
				applied := false
				for _, item := range batch {
					op, ok := item.(map[string]interface{})
					if !ok {
						continue
					}
					subPath, _ := op["p"].(string)
					subOp, _ := op["o"].(string)
					if sseparser.ApplyPatch(state, subPath, subOp, op["v"]) {
						applied = true
					}
				}
				if applied {
					return conversationStreamEvent{response: state.Response, messageID: state.Response.Message.ID, channel: state.Channel}, true
				}
			}
		}
		if sseparser.ApplyPatch(state, patchPath, patchOperation, raw["v"]) {
			return conversationStreamEvent{response: state.Response, messageID: state.Response.Message.ID, channel: state.Channel}, true
		}
	}

	return conversationStreamEvent{}, false
}

// Handler 处理对话响应（简化版）。
func Handler(c *gin.Context, response *http.Response, client httpclient.AuroraHttpClient, account *accounts.Account, uuid string, translated_request chatgpt_types.ChatGPTRequest, stream bool, model string) (string, *ContinueInfo) {
	result := HandlerDetailed(c, response, client, account, uuid, translated_request, stream, model)
	if result.Err != nil {
		httpstream.WriteChatCompletionError(c, result.Err)
		return "", nil
	}
	return result.Text, result.Continue
}

// HandlerDetailed 处理对话响应（详细版）。
func HandlerDetailed(c *gin.Context, response *http.Response, client httpclient.AuroraHttpClient, account *accounts.Account, uuid string, translated_request chatgpt_types.ChatGPTRequest, stream bool, model string) HandlerResult {
	return HandlerDetailedWithWebsocket(c, response, client, account, uuid, translated_request, stream, model, nil)
}

// HandlerDetailedWithWebsocket 处理对话响应（带 WebSocket）。
func HandlerDetailedWithWebsocket(c *gin.Context, response *http.Response, client httpclient.AuroraHttpClient, account *accounts.Account, uuid string, translated_request chatgpt_types.ChatGPTRequest, stream bool, model string, wsConn *websocket.Conn) HandlerResult {
	return HandlerDetailedWithOptions(c, response, client, account, uuid, translated_request, stream, model, HandlerDetailedOptions{Websocket: wsConn})
}

// HandlerDetailedOptions 是 HandlerDetailedWithOptions 的可选参数。
type HandlerDetailedOptions struct {
	Websocket        *websocket.Conn
	ClientState      *ChatClientState
	ArtifactDelivery string
	ProxyURL         string
	Tools            []official_types.Tool
	SuppressOutput   bool
	// DeferTerminal lets the caller durably checkpoint before reporting success.
	DeferTerminal bool
}

// HandlerDetailedWithOptions 处理对话响应流（最完整版）。
func HandlerDetailedWithOptions(c *gin.Context, response *http.Response, client httpclient.AuroraHttpClient, account *accounts.Account, uuid string, translated_request chatgpt_types.ChatGPTRequest, stream bool, model string, options HandlerDetailedOptions) (result HandlerResult) {
	if model == "" {
		model = translated_request.Model
	}
	wsConn := options.Websocket
	if options.ClientState != nil {
		options.ClientState.ApplyToRequest(&translated_request)
	}
	max_tokens := false

	reader := bufio.NewReader(response.Body)
	if wsConn != nil {
		// The orchestration layer may establish this connection for a
		// non-streaming extended/max request before posting the conversation.
		defer wsConn.Close()
	} else if stream && client != nil && account != nil {
		// Preserve the fallback for streaming callers that did not establish a
		// WebSocket before entering the response handler.
		if conn, err := DialChatWebsocketWithStateAndProxy(client, account, options.ClientState, options.ProxyURL); err == nil {
			wsConn = conn
			defer wsConn.Close()
		}
	}

	streamOutput := stream && !options.SuppressOutput
	if streamOutput {
		c.Header("Content-Type", "text/event-stream")
		c.Header("Cache-Control", "no-cache")
		c.Header("Connection", "keep-alive")
		c.Header("X-Accel-Buffering", "no")
	} else {
		c.Header("Content-Type", "application/json")
	}
	var finish_reason string
	var previous_text typings.StringStruct
	var visibleText strings.Builder
	var generatedImageText strings.Builder
	var usedChunkEvents bool
	var original_response chatgpt_types.ChatGPTResponse
	var isRole = true
	var waitSource = false
	var isEnd = false
	var deferredTerminal *official_types.ChatCompletionChunk
	emitTerminal := func(chunk official_types.ChatCompletionChunk) {
		if options.DeferTerminal {
			deferredTerminal = &chunk
			return
		}
		c.Writer.WriteString("data: " + chunk.String() + "\n\n")
		c.Writer.Flush()
	}
	defer func() {
		result.Completed = result.Err == nil && isEnd && !max_tokens && finish_reason != "length" &&
			result.ConversationID != "" && result.ParentMessageID != ""
		result.DeferredTerminal = deferredTerminal
		if deferredTerminal != nil {
			result.StopSent = false
		}
	}()
	var imgSource []string
	renderedImageFiles := make(map[string]bool)
	var convId string
	var sentinel []map[string]interface{}
	var thinkingText string
	var activeChannel string
	var assistantMessageID string
	artifactState := newArtifactAccumulator()
	artifactConfig := ArtifactStreamConfig{Delivery: options.ArtifactDelivery}
	var patchState sseparser.PatchState
	var citePipeline sseparser.CiteStreamPipeline
	var handoffTopicID string
	var currentEvent string
	var readingWebsocket bool
	var readingSnapshot bool
	var websocketStream io.ReadCloser
	emitSentinels := func(items []map[string]interface{}) {
		if len(items) == 0 {
			return
		}
		sentinel = append(sentinel, items...)
		if !streamOutput {
			return
		}
		for _, item := range items {
			chunk := official_types.NewChatCompletionChunk("", model)
			if convId != "" {
				chunk.ConversationID = convId
			}
			chunk.Sentinel = item
			c.Writer.WriteString("data: " + chunk.String() + "\n\n")
			c.Writer.Flush()
		}
	}
	observeArtifacts := func(line string) {
		var raw map[string]interface{}
		if err := json.Unmarshal([]byte(line), &raw); err != nil {
			return
		}
		if cid := firstConversationID(raw); cid != "" && convId == "" {
			convId = cid
		}
		events := artifactState.ObserveRaw(raw, convId)
		emitSentinels(materializeArtifactEvents(client, account, convId, events, artifactConfig))
		if artifactState.LastAssistantMsgID != "" {
			assistantMessageID = artifactState.LastAssistantMsgID
		}
		if artifactState.ConversationID != "" && convId == "" {
			convId = artifactState.ConversationID
		}
	}
	emitThinking := func(delta string) {
		if delta == "" {
			return
		}
		thinkingText += delta
		emitSentinels([]map[string]interface{}{{
			"event": "thinking",
			"kind":  "analysis",
			"delta": delta,
		}})
		if streamOutput {
			reasoningChunk := official_types.NewReasoningChunk(delta, model)
			if convId != "" {
				reasoningChunk.ConversationID = convId
			}
			c.Writer.WriteString("data: " + reasoningChunk.String() + "\n\n")
			c.Writer.Flush()
		}
	}
	publicSummaries := make(map[string]string)
	emitSummary := func(key, text string) {
		if text == "" || publicSummaries[key] == text {
			return
		}
		previous := publicSummaries[key]
		if previous == "" && thinkingText != "" {
			emitThinking("\n\n")
		}
		emitThinking(sseparser.NormalizeContentDelta(previous, text))
		publicSummaries[key] = text
	}
	emitPublicActivity := func(message chatgpt_types.Message) bool {
		if message.Author.Role == "assistant" {
			switch message.Content.ContentType {
			case "thoughts":
				for index, thought := range message.Content.Thoughts {
					emitSummary(fmt.Sprintf("%s:thought:%d", message.ID, index), thought.Summary)
				}
				return true
			case "reasoning_recap":
				emitSummary(message.ID+":recap", message.Content.Recap)
				return true
			}
		}
		name, _ := message.Author.Name.(string)
		isWeb := func(value string) bool {
			return value == "web" || value == "browser" ||
				strings.HasPrefix(value, "web.") || strings.HasPrefix(value, "browser.")
		}
		if (message.Author.Role == "assistant" && isWeb(message.Recipient)) ||
			(message.Author.Role == "tool" && isWeb(name)) {
			// This describes an upstream operation, not a downstream tool call.
			emitSummary("web-activity", "Searching the web.")
		}
		return false
	}
	finalizeArtifacts := func() {
		emitSentinels(materializeArtifactEvents(client, account, convId, artifactState.Finalize(), artifactConfig))
		if markdown := finalGeneratedImageMarkdown(sentinel, renderedImageFiles); markdown != "" {
			generatedImageText.WriteString(markdown)
			if streamOutput {
				chunk := official_types.NewChatCompletionChunk(markdown, model)
				chunk.ConversationID = convId
				c.Writer.WriteString("data: " + chunk.String() + "\n\n")
				c.Writer.Flush()
			}
		}
	}
	finalText := func() string {
		if usedChunkEvents {
			return visibleText.String() + generatedImageText.String()
		}
		return sseparser.ReplaceCiteMarkers(previous_text.Text, patchState.CiteAlts) + generatedImageText.String()
	}
	flushCites := func() {
		flushed := citePipeline.Flush(patchState.CiteAlts)
		if flushed == "" {
			return
		}
		if streamOutput {
			flushChunk := official_types.NewChatCompletionChunk(flushed, model)
			flushChunk.ConversationID = convId
			c.Writer.WriteString("data: " + flushChunk.String() + "\n\n")
			c.Writer.Flush()
		}
		if usedChunkEvents {
			visibleText.WriteString(flushed)
		}
	}
	incompleteHandoff := func() bool {
		return handoffTopicID != "" && !isEnd && !readingSnapshot
	}
	recoverHandoff := func() bool {
		// Replaying a partial answer would duplicate already-delivered content.
		// Report failure explicitly instead of silently truncating that answer.
		if translated_request.Action == "continue" || previous_text.Text != "" || visibleText.Len() != 0 || len(imgSource) != 0 || generatedImageText.Len() != 0 {
			return false
		}
		ctx := context.Background()
		if c.Request != nil {
			ctx = c.Request.Context()
		}
		// Match the transport's allowance for long-running background turns.
		ctx, cancel := context.WithTimeout(ctx, 600*time.Second)
		defer cancel()
		expectedUserID := ""
		for i := len(translated_request.Messages) - 1; i >= 0; i-- {
			if translated_request.Messages[i].Author.Role == "user" {
				expectedUserID = translated_request.Messages[i].ID.String()
				break
			}
		}
		var beforePoll func()
		if stream {
			// Comments keep both Chat Completions and Responses streams alive
			// without inventing reasoning or writing the caller's protocol.
			beforePoll = func() {
				c.Writer.WriteString(": keep-alive\n\n")
				c.Writer.Flush()
			}
		}
		onProgress := func(body []byte) {
			for _, payload := range sseparser.DataPayloads(string(body)) {
				var response chatgpt_types.ChatGPTResponse
				if json.Unmarshal([]byte(payload), &response) == nil {
					emitPublicActivity(response.Message)
				}
			}
		}
		body, err := getCompletedConversation(ctx, client, account, convId, expectedUserID, beforePoll, onProgress)
		if err != nil {
			return false
		}
		reader = bufio.NewReader(bytes.NewReader(body))
		readingSnapshot = true
		usedChunkEvents = false
		patchState = sseparser.PatchState{}
		activeChannel = ""
		currentEvent = ""
		return true
	}
readLoop:
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if incompleteHandoff() {
				if !recoverHandoff() {
					return HandlerResult{Err: ErrIncompleteHandoff}
				}
				continue readLoop
			}
			if err == io.EOF && line == "" {
				break
			}
			if err != io.EOF {
				return HandlerResult{}
			}
		}
		if eventName, ok := sseparser.EventName(line); ok {
			currentEvent = eventName
		}
		for _, line := range sseparser.DataPayloads(line) {
			if strings.HasPrefix(line, "[DONE]") {
				if shouldUseWebsocketHandoff(readingWebsocket, handoffTopicID, wsConn, previous_text.Text, imgSource) {
					wsReader, err := chatWebsocketStreamReader(wsConn, handoffTopicID)
					if err == nil {
						websocketStream = wsReader
						defer websocketStream.Close()
						reader = bufio.NewReader(wsReader)
						readingWebsocket = true
						currentEvent = ""
						continue readLoop
					}
				}
				if incompleteHandoff() {
					if !recoverHandoff() {
						return HandlerResult{Err: ErrIncompleteHandoff}
					}
					continue readLoop
				}
				flushCites()
				finalizeArtifacts()
				break readLoop
			}
			observeArtifacts(line)
			if topicID, skip := sseparser.HandoffTopicFromPayload(line, currentEvent); skip {
				if topicID != "" {
					handoffTopicID = topicID
				}
				currentEvent = ""
				continue
			}
			streamEvent, ok := parseConversationEvent(line, &patchState, model)
			if os.Getenv("DEBUG_SSE") != "" {
				debugText := streamEvent.text
				debugSrc := "chunk"
				if streamEvent.response.Message.ID != "" {
					debugText = sseparser.FirstStringPart(streamEvent.response.Message.Content.Parts)
					debugSrc = "response"
				}
				raw := strings.TrimSpace(line)
				if len(raw) > 200 {
					raw = raw[:200] + "..."
				}
				fmt.Printf("[sse-in] src=%s channel=%q textLen=%d finish=%q parsed=%v raw=%q\n", debugSrc, streamEvent.channel, len(debugText), streamEvent.finishReason, ok, raw)
			}
			if !ok {
				currentEvent = ""
				continue
			}
			if streamEvent.chunk != nil {
				usedChunkEvents = true
				if streamEvent.conversationID != "" {
					convId = streamEvent.conversationID
				}
				if streamEvent.chunk.Sentinel != nil {
					sentinel = append(sentinel, streamEvent.chunk.Sentinel)
				}
				deltaText := sseparser.NormalizeContentDelta(previous_text.Text, streamEvent.text)
				// cite 标记流式处理
				deltaText = citePipeline.Feed(patchState.CiteAlts, deltaText)
				if streamEvent.channel != "" {
					activeChannel = streamEvent.channel
				}
				if streamEvent.finishReason != "" {
					finish_reason = streamEvent.finishReason
					if finish_reason == "length" {
						max_tokens = true
					}
					isEnd = true
				}
				if activeChannel == "analysis" {
					emitThinking(streamEvent.text)
					if streamEvent.isStop {
						finalizeArtifacts()
						if streamOutput {
							finalLine := official_types.StopChunkWithConversation(finish_reason, model, convId)
							emitTerminal(finalLine)
						}
						if max_tokens && convId != "" && assistantMessageID != "" {
							return HandlerResult{
								Text:              strings.Join(imgSource, "") + finalText(),
								ThinkingText:      thinkingText,
								ConversationID:    convId,
								ParentMessageID:   assistantMessageID,
								Sentinel:          sentinel,
								ArtifactSignals:   artifactState.Signals,
								SandboxArtifacts:  artifactState.SandboxArtifacts,
								PDFArtifacts:      artifactState.PDFArtifacts,
								GeneratedImageIDs: artifactState.ImageFileIDs,
								StopSent:          true,
								Continue: &ContinueInfo{
									ConversationID: convId,
									ParentID:       assistantMessageID,
								},
							}
						}
						return HandlerResult{
							Text:              strings.Join(imgSource, "") + finalText(),
							ThinkingText:      thinkingText,
							ConversationID:    convId,
							ParentMessageID:   assistantMessageID,
							Sentinel:          sentinel,
							ArtifactSignals:   artifactState.Signals,
							SandboxArtifacts:  artifactState.SandboxArtifacts,
							PDFArtifacts:      artifactState.PDFArtifacts,
							GeneratedImageIDs: artifactState.ImageFileIDs,
							StopSent:          true,
						}
					}
					currentEvent = ""
					continue
				}
				var terminalChunk *official_types.ChatCompletionChunk
				if streamOutput {
					outChunk := *streamEvent.chunk
					if len(outChunk.Choices) > 0 {
						outChunk.Choices[0].Delta.Content = deltaText
						if streamEvent.role == "" || !isRole {
							outChunk.Choices[0].Delta.Role = ""
						}
					}
					if streamEvent.isStop {
						terminal := outChunk
						terminal.Choices = append([]official_types.Choices(nil), outChunk.Choices...)
						if terminal.ConversationID == "" {
							terminal.ConversationID = convId
						}
						if len(terminal.Choices) > 0 {
							terminal.Choices[0].Delta = official_types.Delta{}
						}
						terminalChunk = &terminal
						if len(outChunk.Choices) > 0 {
							outChunk.Choices[0].FinishReason = nil
						}
					}
					shouldWrite := deltaText != "" ||
						(streamEvent.role != "" && isRole) ||
						streamEvent.chunk.Sentinel != nil
					if shouldWrite {
						c.Writer.WriteString("data: " + outChunk.String() + "\n\n")
						c.Writer.Flush()
					}
					if streamEvent.role != "" && isRole {
						isRole = false
					}
				}
				if deltaText != "" {
					previous_text.Text += deltaText
					visibleText.WriteString(deltaText)
				}
				if streamEvent.isStop {
					flushCites()
					finalizeArtifacts()
					if terminalChunk != nil {
						emitTerminal(*terminalChunk)
					}
					if max_tokens && convId != "" && assistantMessageID != "" {
						return HandlerResult{
							Text:              strings.Join(imgSource, "") + finalText(),
							ThinkingText:      thinkingText,
							ConversationID:    convId,
							ParentMessageID:   assistantMessageID,
							Sentinel:          sentinel,
							ArtifactSignals:   artifactState.Signals,
							SandboxArtifacts:  artifactState.SandboxArtifacts,
							PDFArtifacts:      artifactState.PDFArtifacts,
							GeneratedImageIDs: artifactState.ImageFileIDs,
							StopSent:          true,
							Continue: &ContinueInfo{
								ConversationID: convId,
								ParentID:       assistantMessageID,
							},
						}
					}
					return HandlerResult{
						Text:              strings.Join(imgSource, "") + finalText(),
						ThinkingText:      thinkingText,
						ConversationID:    convId,
						ParentMessageID:   assistantMessageID,
						Sentinel:          sentinel,
						ArtifactSignals:   artifactState.Signals,
						SandboxArtifacts:  artifactState.SandboxArtifacts,
						PDFArtifacts:      artifactState.PDFArtifacts,
						GeneratedImageIDs: artifactState.ImageFileIDs,
						StopSent:          true,
					}
				}
				currentEvent = ""
				continue
			}
			original_response = streamEvent.response
			if original_response.Error != nil {
				c.JSON(500, gin.H{"error": original_response.Error})
				return HandlerResult{}
			}
			sentinel = append(sentinel, sseparser.SentinelsFromResponse(original_response)...)
			if original_response.ConversationID != convId {
				if convId == "" {
					convId = original_response.ConversationID
				} else {
					continue
				}
			}
			if streamEvent.channel != "" {
				activeChannel = streamEvent.channel
			}
			if original_response.Message.ID != "" && (original_response.Message.Author.Role == "assistant" || original_response.Message.Author.Role == "tool") {
				assistantMessageID = original_response.Message.ID
			}
			if emitPublicActivity(original_response.Message) {
				currentEvent = ""
				continue
			}
			if activeChannel == "analysis" {
				thinkingDelta := sseparser.NormalizeContentDelta(thinkingText, sseparser.FirstStringPart(original_response.Message.Content.Parts))
				emitThinking(thinkingDelta)
				currentEvent = ""
				continue
			}
			if !(original_response.Message.Author.Role == "assistant" || (original_response.Message.Author.Role == "tool" && original_response.Message.Content.ContentType != "text")) || original_response.Message.Content.Parts == nil {
				continue
			}
			if original_response.Message.Metadata.IsThinkingPreambleMessage {
				continue
			}
			if original_response.Message.Channel == "commentary" {
				continue
			}
			if original_response.Message.Metadata.MessageType == "" && activeChannel != "final" {
				continue
			}
			if (original_response.Message.Metadata.MessageType != "next" && original_response.Message.Metadata.MessageType != "continue" && activeChannel != "final") || !strings.HasSuffix(original_response.Message.Content.ContentType, "text") {
				continue
			}
			if endTurn, ok := original_response.Message.EndTurn.(bool); ok && endTurn {
				if waitSource {
					waitSource = false
				}
				isEnd = true
			}
			if len(original_response.Message.Metadata.Citations) != 0 {
				r := []rune(original_response.Message.Content.Parts[0].(string))
				if waitSource {
					if string(r[len(r)-1:]) == "】" {
						waitSource = false
					} else {
						continue
					}
				}
				offset := 0
				for _, citation := range original_response.Message.Metadata.Citations {
					rl := len(r)
					attr := urlAttrMap[citation.Metadata.URL]
					if attr == "" {
						u, _ := url.Parse(citation.Metadata.URL)
						BaseURL := u.Scheme + "://" + u.Host + "/"
						attr = getURLAttribution(client, account, BaseURL)
						if attr != "" {
							urlAttrMap[citation.Metadata.URL] = attr
						}
					}
					original_response.Message.Content.Parts[0] = string(r[:citation.StartIx+offset]) + " ([" + attr + "](" + citation.Metadata.URL + " \"" + citation.Metadata.Title + "\"))" + string(r[citation.EndIx+offset:])
					r = []rune(original_response.Message.Content.Parts[0].(string))
					offset += len(r) - rl
				}
			} else if waitSource {
				continue
			}
			response_string := ""
			if original_response.Message.Recipient != "all" {
				continue
			}
			if original_response.Message.Content.ContentType == "multimodal_text" {
				apiUrl := BaseURL + "/files/"
				if FILES_REVERSE_PROXY != "" {
					apiUrl = FILES_REVERSE_PROXY
				}
				imgSource = make([]string, len(original_response.Message.Content.Parts))
				legacyImageFiles := make([]string, len(imgSource))
				var wg sync.WaitGroup
				for index, part := range original_response.Message.Content.Parts {
					jsonItem, _ := json.Marshal(part)
					var dalle_content chatgpt_types.DalleContent
					err = json.Unmarshal(jsonItem, &dalle_content)
					if err != nil {
						continue
					}
					legacyImageFiles[index] = extractFileID(dalle_content.AssetPointer)
					url := apiUrl + strings.Split(dalle_content.AssetPointer, "//")[1] + "/download"
					wg.Add(1)
					go GetImageSource(client, &wg, url, dalle_content.Metadata.Dalle.Prompt, account, index, imgSource)
				}
				wg.Wait()
				for index, image := range imgSource {
					if image != "" && legacyImageFiles[index] != "" {
						renderedImageFiles[legacyImageFiles[index]] = true
					}
				}
				translated_response := official_types.NewChatCompletionChunk(strings.Join(imgSource, ""), model)
				if isRole {
					translated_response.Choices[0].Delta.Role = original_response.Message.Author.Role
				}
				response_string = "data: " + translated_response.String() + "\n\n"
			}
			if response_string == "" {
				response_string = chatgpt.ConvertToString(&original_response, &previous_text, isRole, model)
				if streamOutput && response_string != "" && strings.HasPrefix(response_string, "data: ") {
					var chunk official_types.ChatCompletionChunk
					if err := json.Unmarshal([]byte(strings.TrimPrefix(response_string, "data: ")), &chunk); err == nil {
						delta := chunk.Choices[0].Delta.Content
						replaced := citePipeline.Feed(patchState.CiteAlts, delta)
						chunk.Choices[0].Delta.Content = replaced
						response_string = "data: " + chunk.String() + "\n\n"
					}
				}
			}
			if response_string == "" {
				if isEnd {
					goto endProcess
				} else {
					continue
				}
			}
			if response_string == "【" {
				waitSource = true
				continue
			}
		endProcess:
			isRole = false
			if streamOutput {
				_, err = c.Writer.WriteString(response_string)
				if err != nil {
					return HandlerResult{}
				}
				c.Writer.Flush()
			}

			if original_response.Message.Metadata.FinishDetails != nil {
				finish_reason = original_response.Message.Metadata.FinishDetails.Type
				if finish_reason == "max_tokens" {
					max_tokens = true
					finish_reason = "length"
				}
			}
			if isEnd {
				if finish_reason == "" {
					finish_reason = "stop"
				}
				flushCites()
				finalizeArtifacts()
				if streamOutput {
					final_line := official_types.StopChunkWithConversation(finish_reason, model, convId)
					emitTerminal(final_line)
				}
				return HandlerResult{
					Text:              strings.Join(imgSource, "") + finalText(),
					ThinkingText:      thinkingText,
					ConversationID:    convId,
					ParentMessageID:   assistantMessageID,
					Sentinel:          sentinel,
					ArtifactSignals:   artifactState.Signals,
					SandboxArtifacts:  artifactState.SandboxArtifacts,
					PDFArtifacts:      artifactState.PDFArtifacts,
					GeneratedImageIDs: artifactState.ImageFileIDs,
					StopSent:          stream,
				}
			}
			currentEvent = ""
		}
		if err == io.EOF {
			if incompleteHandoff() {
				if !recoverHandoff() {
					return HandlerResult{Err: ErrIncompleteHandoff}
				}
				continue readLoop
			}
			break
		}
	}
	flushCites()
	if !max_tokens {
		finalizeArtifacts()
		return HandlerResult{
			Text:              strings.Join(imgSource, "") + finalText(),
			ThinkingText:      thinkingText,
			ConversationID:    convId,
			ParentMessageID:   assistantMessageID,
			Sentinel:          sentinel,
			ArtifactSignals:   artifactState.Signals,
			SandboxArtifacts:  artifactState.SandboxArtifacts,
			PDFArtifacts:      artifactState.PDFArtifacts,
			GeneratedImageIDs: artifactState.ImageFileIDs,
		}
	}
	finalizeArtifacts()
	return HandlerResult{
		Text:              strings.Join(imgSource, "") + finalText(),
		ThinkingText:      thinkingText,
		ConversationID:    convId,
		ParentMessageID:   assistantMessageID,
		Sentinel:          sentinel,
		ArtifactSignals:   artifactState.Signals,
		SandboxArtifacts:  artifactState.SandboxArtifacts,
		PDFArtifacts:      artifactState.PDFArtifacts,
		GeneratedImageIDs: artifactState.ImageFileIDs,
		Continue: &ContinueInfo{
			ConversationID: original_response.ConversationID,
			ParentID:       original_response.Message.ID,
		},
	}
}
