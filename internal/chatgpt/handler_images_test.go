package chatgpt

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"

	"aurora/httpclient"
	"aurora/internal/accounts"
	"aurora/internal/httpstream"

	"github.com/gin-gonic/gin"
)

// Only URL resolution is allowed. In particular, this client cannot contact
// an upstream model, fetch image bytes, or establish a WebSocket.
type generatedImageHandlerClient struct {
	httpclient.AuroraHttpClient
	t    *testing.T
	urls map[string]string
}

func (c *generatedImageHandlerClient) Request(method httpclient.HttpMethod, url string, headers httpclient.AuroraHeaders, cookies []*http.Cookie, body io.Reader) (*http.Response, error) {
	if method == http.MethodGet && body == nil {
		for fileID, downloadURL := range c.urls {
			if url == fileDownloadBaseURL()+fileID+"/download" {
				payload, err := json.Marshal(map[string]string{"status": "success", "download_url": downloadURL})
				if err != nil {
					return nil, err
				}
				return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(string(payload)))}, nil
			}
		}
	}
	c.t.Errorf("unexpected image handler request: %s %s", method, url)
	return nil, fmt.Errorf("unexpected image handler request: %s %s", method, url)
}

func TestHandlerGeneratedImagesInNormalContent(t *testing.T) {
	gin.SetMode(gin.TestMode)
	const finalURL = "https://example.invalid/generated.png"
	const draftURL = "https://example.invalid/draft.png"
	imageMarkdown := regexp.MustCompile(`!\[[^\]\r\n]*\]\(https://example\.invalid/generated\.png(?: "[^"\r\n]*")?\)`)
	cases := []struct {
		name      string
		files     []string
		revisions int
		caption   string
		legacy    bool
	}{
		{name: "single", files: []string{"file-final"}, revisions: 1},
		{name: "legacy_duplicate", files: []string{"file-final"}, revisions: 1, legacy: true},
		{name: "caption", files: []string{"file-final"}, revisions: 1, caption: "Here is your image."},
		{name: "repeated", files: []string{"file-final", "file-final"}, revisions: 1},
		{name: "revised_slot", files: []string{"file-draft", "file-final", "file-final"}, revisions: 2},
	}
	for _, tc := range cases {
		for _, mode := range []struct {
			name             string
			stream, suppress bool
		}{{name: "nonstream"}, {name: "stream", stream: true}, {name: "suppressed", stream: true, suppress: true}} {
			t.Run(tc.name+"/"+mode.name, func(t *testing.T) {
				stream := mode.stream
				var upstream strings.Builder
				for _, fileID := range tc.files {
					// A modern nested image pointer, not legacy multimodal_text.
					// Both revisions share generation and slot identity.
					fmt.Fprintf(&upstream, `data: {"v":{"conversation_id":"conv-image","message":{"id":"msg-image","author":{"role":"assistant"},"recipient":"all","content":{"content_type":"image_asset_pointer","parts":[{"asset_pointer":"sediment://%s"}]},"end_turn":false,"metadata":{"message_type":"next","image_gen_task_id":"task-image","dalle":{"gen_id":"gen-image","slot_index":1}}}}}`+"\n\n", fileID)
				}
				if tc.legacy {
					upstream.WriteString(`data: {"conversation_id":"conv-image","message":{"id":"msg-image","author":{"role":"assistant"},"recipient":"all","content":{"content_type":"multimodal_text","parts":[{"asset_pointer":"sediment://file-final"}]},"end_turn":false,"metadata":{"message_type":"next","image_gen_task_id":"task-image","dalle":{"gen_id":"gen-image","slot_index":1}}}}` + "\n\n")
				}
				fmt.Fprintf(&upstream, `data: {"conversation_id":"conv-image","message":{"id":"msg-image","author":{"role":"assistant"},"recipient":"all","content":{"content_type":"text","parts":[%q]},"end_turn":true,"metadata":{"message_type":"next","finish_details":{"type":"stop"}}}}`+"\n\ndata: [DONE]\n\n", tc.caption)
				response := &http.Response{Body: io.NopCloser(strings.NewReader(upstream.String()))}
				defer response.Body.Close()
				writer := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(writer)
				client := &generatedImageHandlerClient{t: t, urls: map[string]string{
					"file-final": finalURL,
					"file-draft": draftURL,
				}}
				// A nil account disables the handler's streaming WebSocket fallback.
				result := HandlerDetailedWithOptions(c, response, client, nil, "request-image", chatGPTRequestForTest(), stream, "auto", HandlerDetailedOptions{ArtifactDelivery: ArtifactDeliveryURL, SuppressOutput: mode.suppress})

				assertImageSentinels := func(items []map[string]interface{}) {
					t.Helper()
					counts := map[string]int{}
					for _, item := range items {
						if item["kind"] != "generated_image" {
							continue
						}
						event, _ := item["event"].(string)
						counts[event]++
						if event == StreamEventArtifact && item["file_id"] == "file-final" && item["url"] != finalURL {
							t.Errorf("final artifact lost download URL: %#v", item)
						}
						if event == StreamEventArtifactSlotFinal && (item["file_id"] != "file-final" || item["is_final"] != true || item["revision"] != float64(tc.revisions)) {
							t.Errorf("wrong final slot metadata: %#v", item)
						}
					}
					for event, want := range map[string]int{
						StreamEventArtifactPending:    1,
						StreamEventArtifact:           tc.revisions,
						StreamEventArtifactSuperseded: tc.revisions - 1,
						StreamEventArtifactSlotFinal:  1,
					} {
						if counts[event] != want {
							t.Errorf("sentinel %s count = %d, want %d: %#v", event, counts[event], want, items)
						}
					}
				}
				assertImageSentinels(result.Sentinel)
				assertImageContent := func(label, text string) {
					t.Helper()
					urlCount := 1
					if tc.legacy {
						urlCount = 2 // The legacy image is wrapped in a link to itself.
					}
					if len(imageMarkdown.FindAllString(text, -1)) != 1 || strings.Count(text, "![") != 1 || strings.Count(text, finalURL) != urlCount || strings.Contains(text, draftURL) {
						t.Errorf("%s = %q, want exactly one Markdown image for final URL and no draft image", label, text)
					}
				}
				assertImageContent("result.Text", result.Text)
				if !stream || mode.suppress {
					if writer.Body.Len() != 0 {
						t.Errorf("nonstream handler wrote response: %s", writer.Body.String())
					}
					return
				}

				httpstream.WriteChatCompletionDone(c, result.StopSent, "auto", result.ConversationID)
				var text strings.Builder
				var sentinels []map[string]interface{}
				for _, chunk := range parseSSEChunks(t, writer.Body.String()) {
					if sentinel, ok := chunk["sentinel"].(map[string]interface{}); ok {
						sentinels = append(sentinels, sentinel)
					}
					choices, _ := chunk["choices"].([]interface{})
					for _, rawChoice := range choices {
						choice := rawChoice.(map[string]interface{})
						delta, _ := choice["delta"].(map[string]interface{})
						content, _ := delta["content"].(string)
						text.WriteString(content)
					}
				}
				assertImageSentinels(sentinels)
				assertImageContent("stream delta.content", text.String())
				// Also verifies one terminal stop, one DONE, and no image content
				// after finish_reason. Sentinel-only delivery cannot satisfy this.
				assertCompletedChatStream(t, writer.Body.String(), result.Text, "stop")
			})
		}
	}
}

func TestFinalGeneratedImageMarkdownRejectsInvalidURLs(t *testing.T) {
	for _, imageURL := range []string{"", "javascript:alert(1)", "data:image/png;base64,test", "/relative/image.png", "https://example.invalid/image\n.png"} {
		t.Run(imageURL, func(t *testing.T) {
			events := []map[string]interface{}{
				{"event": StreamEventArtifact, "kind": "generated_image", "file_id": "file-test", "url": imageURL},
				{"event": StreamEventArtifactSlotFinal, "kind": "generated_image", "file_id": "file-test"},
			}
			if got := finalGeneratedImageMarkdown(events, make(map[string]bool)); got != "" {
				t.Fatalf("rendered invalid image URL: %q", got)
			}
		})
	}
}

func TestImageProxyURLHooks(t *testing.T) {
	const nativeURL = "https://example.invalid/native.png"
	const capabilityURL = "https://aurora.invalid/images/capability"
	account := &accounts.Account{}
	client := &generatedImageHandlerClient{t: t, urls: map[string]string{"file-proxy": nativeURL}}
	for _, mode := range []string{"disabled", "unsupported", "supported"} {
		t.Run(mode, func(t *testing.T) {
			previous := ImageProxyURL
			t.Cleanup(func() { ImageProxyURL = previous })
			ImageProxyURL = nil
			calls := 0
			if mode != "disabled" {
				ImageProxyURL = func(gotAccount *accounts.Account, fileID string) string {
					calls++
					if gotAccount != account || fileID != "file-proxy" {
						t.Fatalf("callback arguments = %p, %q", gotAccount, fileID)
					}
					if mode == "supported" {
						return capabilityURL
					}
					return ""
				}
			}
			wantURL := nativeURL
			if mode == "supported" {
				wantURL = capabilityURL
			}
			images := make([]string, 1)
			var wg sync.WaitGroup
			wg.Add(1)
			GetImageSource(client, &wg, fileDownloadBaseURL()+"file-proxy/download", "prompt", account, 0, images)
			wg.Wait()
			if want := "[![image](" + wantURL + " \"prompt\")](" + wantURL + ")"; images[0] != want {
				t.Fatalf("legacy Markdown = %q, want %q", images[0], want)
			}
			ev := StreamEvent{Event: StreamEventArtifact, Kind: "generated_image", FileID: "file-proxy"}
			events := materializeGeneratedImageEvent(client, account, "conv-proxy", ev, ArtifactStreamConfig{Delivery: ArtifactDeliveryURL})
			if len(events) != 1 || events[0]["url"] != wantURL {
				t.Fatalf("URL artifact = %#v", events)
			}
			// Shared resolution must remain native for authenticated byte consumers.
			if url, err := ResolveGeneratedImageURL(client, account, "conv-proxy", ev.FileID); err != nil || url != nativeURL {
				t.Fatalf("native resolution = %q, %v", url, err)
			}
			wantCalls := 2
			if mode == "disabled" {
				wantCalls = 0
			}
			if calls != wantCalls {
				t.Fatalf("callback calls = %d, want %d", calls, wantCalls)
			}
		})
	}
}

type imageProxyBytesClient struct {
	generatedImageHandlerClient
}

func (c *imageProxyBytesClient) Request(method httpclient.HttpMethod, url string, headers httpclient.AuroraHeaders, cookies []*http.Cookie, body io.Reader) (*http.Response, error) {
	if method == httpclient.GET && url == "https://example.invalid/native.png" && body == nil {
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("image bytes"))}, nil
	}
	return c.generatedImageHandlerClient.Request(method, url, headers, cookies, body)
}

func TestImageProxyURLDoesNotAffectByteDelivery(t *testing.T) {
	previous := ImageProxyURL
	t.Cleanup(func() { ImageProxyURL = previous })
	ImageProxyURL = func(*accounts.Account, string) string {
		t.Fatal("byte delivery must not invoke the image URL proxy")
		return ""
	}
	client := &imageProxyBytesClient{generatedImageHandlerClient{t: t, urls: map[string]string{"file-proxy": "https://example.invalid/native.png"}}}
	for _, delivery := range []string{ArtifactDeliveryBase64, ArtifactDeliveryBase64Chunked} {
		t.Run(delivery, func(t *testing.T) {
			ev := StreamEvent{Event: StreamEventArtifact, Kind: "generated_image", FileID: "file-proxy"}
			events := materializeGeneratedImageEvent(client, nil, "conv-proxy", ev, ArtifactStreamConfig{Delivery: delivery}.normalized())
			if len(events) == 0 {
				t.Fatal("missing byte delivery events")
			}
			if events[0]["url"] != "https://example.invalid/native.png" {
				t.Fatalf("byte delivery lost native URL: %#v", events)
			}
			dataIndex := 0
			if delivery == ArtifactDeliveryBase64Chunked {
				dataIndex = 1
			}
			if len(events) <= dataIndex || events[dataIndex]["data"] != "aW1hZ2UgYnl0ZXM=" {
				t.Fatalf("byte delivery lost image data: %#v", events)
			}
			for _, event := range events {
				if event["error"] != nil && event["error"] != "" {
					t.Fatalf("byte delivery failed: %#v", events)
				}
			}
		})
	}
}
