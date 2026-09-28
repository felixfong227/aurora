package handler

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"aurora/internal/accounts"
	"aurora/internal/config"

	"github.com/gin-gonic/gin"
)

func relayTestAccount(user, nonce string) *accounts.Account {
	claims, _ := json.Marshal(map[string]interface{}{
		"https://api.openai.com/auth": map[string]string{
			"chatgpt_account_id": "test-workspace", "chatgpt_user_id": user,
		},
		"nonce": nonce,
	})
	token := "test." + base64.RawURLEncoding.EncodeToString(claims) + ".test"
	account := accounts.NewAccount(nonce, accounts.TypeFree, token)
	account.Status = accounts.StatusActive
	return account
}

func relayTestProxy(t *testing.T, backend string, account *accounts.Account) *imageProxy {
	t.Helper()
	p, err := newImageProxy(accounts.NewPool([]*accounts.Account{account}), &config.Config{
		BaseURL: backend + "/backend-api", ImageProxyPublicURL: "https://aurora.example",
		Authorization: strings.Repeat("test-only-key-", 4),
	})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func relayTestRouter(p *imageProxy, logs io.Writer) *gin.Engine {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(gin.LoggerWithConfig(gin.LoggerConfig{Output: logs, Skip: skipImageProxyLog}))
	router.GET(imageProxyPrefix+":owner/:file/:signature", gin.RecoveryWithWriter(io.Discard), p.Serve)
	return router
}

func TestImageProxyStreamsWithoutExposingCredentials(t *testing.T) {
	account := relayTestAccount("user-a", "old-token")
	var server *httptest.Server
	var paths []string
	// Synthetic raster bytes; the relay must stream them unchanged.
	image := []byte("\x89PNG\r\n\x1a\nsynthetic-image")
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		if r.Method != http.MethodGet || r.Header.Get("Authorization") != "Bearer "+account.Token {
			t.Error("upstream request lacked the selected account's authorization")
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		switch r.URL.Path {
		case "/backend-api/files/file-example/download":
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]string{"download_url": server.URL + "/backend-api/estuary/content?id=file-example"})
		case "/backend-api/estuary/content":
			w.Header().Set("Content-Type", "image/png")
			w.Write(image)
		default:
			t.Error("unexpected upstream path")
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	p := relayTestProxy(t, server.URL, account)
	link := p.URL(account, "file-example")
	if link == "" || strings.Contains(link, account.Token) || strings.Contains(link, string(p.key)) {
		t.Fatal("image URL missing or exposing an account/service credential")
	}
	var logs bytes.Buffer
	router := relayTestRouter(p, &logs)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, link, nil))
	if response.Code != http.StatusOK || !bytes.Equal(response.Body.Bytes(), image) {
		t.Fatalf("image not relayed unchanged: status=%d bytes=%d", response.Code, response.Body.Len())
	}
	if len(paths) != 2 || response.Header().Get("Cache-Control") != "no-store" ||
		response.Header().Get("Referrer-Policy") != "no-referrer" || logs.Len() != 0 {
		t.Fatal("relay requested extra data, cached the image, or logged a capability")
	}
}

func TestImageProxyRejectsTamperingBeforeNetwork(t *testing.T) {
	account := relayTestAccount("user-a", "token-a")
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer server.Close()
	p := relayTestProxy(t, server.URL, account)
	owner := accounts.ImageIdentity(account, p.key)
	signature := p.signature(owner, "file-example")
	var logs bytes.Buffer
	router := relayTestRouter(p, &logs)
	for _, suffix := range []string{
		owner + "/file-other/" + signature,
		owner + "/file-example/" + strings.Repeat("0", 64),
		strings.Repeat("0", 64) + "/file-example/" + signature,
		owner + "/not-a-file/" + signature,
	} {
		response := httptest.NewRecorder()
		router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, imageProxyPrefix+suffix, nil))
		if response.Code != http.StatusNotFound {
			t.Fatalf("invalid capability returned %d", response.Code)
		}
	}
	if logs.Len() != 0 || calls != 0 {
		t.Fatal("invalid image capabilities were logged or reached upstream")
	}
}

func TestImageProxyLinksSurviveRenewalButNotAccountChanges(t *testing.T) {
	first := relayTestAccount("user-a", "process-one")
	p := relayTestProxy(t, "https://backend.example", first)
	link := p.URL(first, "file-example")
	renewed := relayTestAccount("user-a", "process-two")
	p.pool = accounts.NewPool([]*accounts.Account{renewed})
	if link == "" || p.URL(renewed, "file-example") != link {
		t.Fatal("token renewal/restart changed the persistent image link")
	}
	otherUser := relayTestAccount("user-b", "process-three")
	if p.URL(otherUser, "file-example") != "" {
		t.Fatal("another user in the workspace matched the configured account")
	}
	renewed.TeamUserID = "another-workspace"
	if p.URL(renewed, "file-example") == link {
		t.Fatal("workspace scope was not included in the capability")
	}
	renewed.IsTemporary = true
	if p.URL(renewed, "file-example") != "" {
		t.Fatal("temporary accounts must not mint persistent capabilities")
	}
	if p.URL(first, "../file-example") != "" {
		t.Fatal("unsafe file path accepted")
	}
}

func TestImageProxyRejectsRedirectsOtherOriginsAndNonImages(t *testing.T) {
	for _, mode := range []string{"metadata_redirect", "other_origin", "other_path", "content_redirect", "html", "svg"} {
		t.Run(mode, func(t *testing.T) {
			account := relayTestAccount("user-a", "token-a")
			calls := 0
			var server *httptest.Server
			server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if strings.HasSuffix(r.URL.Path, "/download") {
					if mode == "metadata_redirect" {
						http.Redirect(w, r, server.URL+"/must-not-follow", http.StatusFound)
						return
					}
					target := server.URL + "/backend-api/estuary/content"
					if mode == "other_origin" {
						target = "https://untrusted.example/backend-api/estuary/content"
					} else if mode == "other_path" {
						target = server.URL + "/backend-api/models"
					}
					json.NewEncoder(w).Encode(map[string]string{"download_url": target})
					return
				}
				if mode == "content_redirect" {
					http.Redirect(w, r, server.URL+"/must-not-follow", http.StatusFound)
					return
				}
				if mode == "svg" {
					w.Header().Set("Content-Type", "image/svg+xml")
				} else {
					w.Header().Set("Content-Type", "text/html")
				}
				w.Write([]byte("not a safe raster image"))
			}))
			defer server.Close()
			p := relayTestProxy(t, server.URL, account)
			response := httptest.NewRecorder()
			relayTestRouter(p, io.Discard).ServeHTTP(response, httptest.NewRequest(http.MethodGet, p.URL(account, "file-example"), nil))
			if response.Code != http.StatusBadGateway || calls > 2 {
				t.Fatalf("unsafe upstream response accepted or redirect followed: status=%d requests=%d", response.Code, calls)
			}
		})
	}
}
