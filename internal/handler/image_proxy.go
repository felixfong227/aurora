package handler

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"aurora/httpclient"
	"aurora/httpclient/bogdanfinn"
	"aurora/internal/accounts"
	"aurora/internal/config"
	"aurora/internal/headerbuilder"

	"github.com/gin-gonic/gin"
)

const imageProxyPrefix = "/image-proxy/"

var imageFileID = regexp.MustCompile(`^file[-_][A-Za-z0-9_-]{1,160}$`)
var imageOwnerID = regexp.MustCompile(`^[a-f0-9]{64}$`)

type imageProxy struct {
	publicURL string
	backend   *url.URL
	key       []byte
	pool      *accounts.Pool
}

func newImageProxy(pool *accounts.Pool, cfg *config.Config) (*imageProxy, error) {
	if cfg.ImageProxyPublicURL == "" {
		return nil, nil
	}
	public, err := url.Parse(cfg.ImageProxyPublicURL)
	if err != nil || !validImageOrigin(public) || public.RawQuery != "" || public.Fragment != "" {
		return nil, errors.New("IMAGE_PROXY_PUBLIC_URL must be an absolute HTTP(S) base URL without credentials, query or fragment")
	}
	backend, err := url.Parse(cfg.BaseURL)
	if err != nil || !validImageOrigin(backend) || backend.RawQuery != "" || backend.Fragment != "" {
		return nil, errors.New("image proxy requires an absolute HTTP(S) BASE_URL without credentials, query or fragment")
	}
	if len(cfg.Authorization) < 32 {
		return nil, errors.New("image proxy requires an Authorization secret of at least 32 characters")
	}
	backend.Path = strings.TrimRight(backend.Path, "/")
	return &imageProxy{
		publicURL: strings.TrimRight(public.String(), "/"),
		backend:   backend,
		key:       []byte(cfg.Authorization),
		pool:      pool,
	}, nil
}

func validImageOrigin(u *url.URL) bool {
	return u != nil && (u.Scheme == "https" || u.Scheme == "http") && u.Host != "" && u.User == nil
}

func (p *imageProxy) signature(owner, fileID string) string {
	mac := hmac.New(sha256.New, p.key)
	mac.Write([]byte("aurora-image-content-v1\n" + owner + "\n" + fileID))
	return hex.EncodeToString(mac.Sum(nil))
}

// URL grants access to one file under one configured account. Links remain valid
// across restarts; rotating Authorization or removing the account revokes them.
// Treat the complete URL as a private bearer capability, not a public file ID.
func (p *imageProxy) URL(account *accounts.Account, fileID string) string {
	if !imageFileID.MatchString(fileID) {
		return ""
	}
	owner := accounts.ImageIdentity(account, p.key)
	if p.pool.FindImageAccount(owner, p.key) == nil {
		return ""
	}
	return p.publicURL + imageProxyPrefix + owner + "/" + fileID + "/" + p.signature(owner, fileID)
}

func (p *imageProxy) Serve(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	c.Header("Referrer-Policy", "no-referrer")
	c.Header("X-Content-Type-Options", "nosniff")
	owner, fileID, signature := c.Param("owner"), c.Param("file"), c.Param("signature")
	if !imageOwnerID.MatchString(owner) || !imageFileID.MatchString(fileID) ||
		!hmac.Equal([]byte(signature), []byte(p.signature(owner, fileID))) {
		c.AbortWithStatus(http.StatusNotFound)
		return
	}
	account := p.pool.FindImageAccount(owner, p.key)
	if account == nil {
		c.AbortWithStatus(http.StatusNotFound)
		return
	}
	// The pool returned a detached snapshot. Never change the shared client's
	// redirect policy, and never let credentials follow a redirect.
	if err := account.InitClient(); err != nil {
		c.AbortWithStatus(http.StatusBadGateway)
		return
	}
	client := account.Client.(*bogdanfinn.TlsClient)
	client.Client.SetFollowRedirect(false)
	defer client.Client.CloseIdleConnections()
	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Minute)
	defer cancel()
	headers := headerbuilder.New().WithBaseHeaders("").
		WithUserAgent(account.Fingerprint.UserAgent).
		WithDeviceID(account.Fingerprint.OaiDeviceID).
		WithSessionID(account.Fingerprint.OaiSessionID).
		WithAuth(account).WithCookies(account).WithTeamAccount(account).
		WithAccept("application/json").Build()
	metadata, err := client.RequestWithContext(ctx, httpclient.GET, p.backend.String()+"/files/"+fileID+"/download", headers, nil, nil)
	if err != nil {
		c.AbortWithStatus(http.StatusBadGateway)
		return
	}
	var file struct {
		DownloadURL string `json:"download_url"`
	}
	if metadata.StatusCode == http.StatusOK {
		err = json.NewDecoder(io.LimitReader(metadata.Body, 64*1024)).Decode(&file)
	}
	metadata.Body.Close()
	target, parseErr := url.Parse(file.DownloadURL)
	// Only the exact authenticated file-content endpoint is allowed. Never
	// forward credentials to a CDN, another origin, or another backend endpoint.
	if err != nil || parseErr != nil || !validImageOrigin(target) ||
		target.Scheme != p.backend.Scheme || target.Host != p.backend.Host ||
		target.Path != p.backend.Path+"/estuary/content" || target.Fragment != "" {
		c.AbortWithStatus(http.StatusBadGateway)
		return
	}
	headers["Accept"] = "image/png,image/jpeg,image/webp,image/gif,image/avif"
	response, err := client.RequestWithContext(ctx, httpclient.GET, target.String(), headers, nil, nil)
	if err != nil {
		c.AbortWithStatus(http.StatusBadGateway)
		return
	}
	defer response.Body.Close()
	contentType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || response.StatusCode != http.StatusOK || !safeImageType(contentType) {
		c.AbortWithStatus(http.StatusBadGateway)
		return
	}
	c.Header("Content-Disposition", "inline")
	c.Header("Content-Security-Policy", "default-src 'none'; sandbox")
	// DataFromReader streams the body with io.Copy; no files or image cache exist.
	c.DataFromReader(http.StatusOK, response.ContentLength, contentType, response.Body, nil)
}

func safeImageType(contentType string) bool {
	switch contentType {
	case "image/png", "image/jpeg", "image/webp", "image/gif", "image/avif":
		return true
	default:
		return false
	}
}

func skipImageProxyLog(c *gin.Context) bool {
	return strings.HasPrefix(c.Request.URL.Path, imageProxyPrefix)
}
