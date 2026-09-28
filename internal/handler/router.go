package handler

import (
	"aurora/httpclient/bogdanfinn"
	"aurora/internal/accounts"
	"aurora/internal/chatgpt"
	"aurora/internal/config"
	"aurora/middlewares"
	"io"

	"github.com/gin-gonic/gin"
)

func optionsHandler(c *gin.Context) {
	c.Header("Access-Control-Allow-Origin", "*")
	c.Header("Access-Control-Allow-Methods", "POST")
	c.Header("Access-Control-Allow-Headers", "*")
	c.JSON(200, gin.H{
		"message": "pong",
	})
}

func RegisterRouter(accountPool *accounts.Pool, cfg *config.Config) *gin.Engine {
	chatHandler := NewChatHandler(accountPool, cfg)
	imageHandler := NewImageHandler(accountPool, cfg)
	audioHandler := NewAudioHandler(accountPool, cfg)
	authHandler := NewAuthHandler(accountPool)
	modelsHandler := NewModelsHandler(accountPool, cfg)
	imageProxy, err := newImageProxy(accountPool, cfg)
	if err != nil {
		panic(err)
	}

	// 初始化基础前置参数（DPL、BasicCookies 等）
	// 用配置的代理访问 chatgpt.com,否则本机连不上 → BasicCookies 收集不到
	proxyUrl := cfg.ProxyURL
	if proxyUrl == "" {
		proxyUrl = cfg.HTTPProxy
	}
	client := bogdanfinn.NewStdClient()
	chatgpt.GetDpl(client, proxyUrl)

	router := gin.New()
	router.Use(gin.LoggerWithConfig(gin.LoggerConfig{Skip: skipImageProxyLog}), gin.Recovery())
	router.Use(middlewares.Cors)
	chatgpt.ImageProxyURL = nil
	if imageProxy != nil {
		chatgpt.ImageProxyURL = imageProxy.URL
		// Suppress request dumps for this capability-bearing route, even on panic.
		router.GET(imageProxyPrefix+":owner/:file/:signature", gin.RecoveryWithWriter(io.Discard), imageProxy.Serve)
	}

	router.GET("/", func(c *gin.Context) { c.JSON(200, gin.H{"message": "Hello, world!"}) })
	router.GET("/ping", func(c *gin.Context) { c.JSON(200, gin.H{"message": "pong"}) })

	router.POST("/auth/session", authHandler.Session)
	router.POST("/auth/refresh", authHandler.Refresh)

	router.OPTIONS("/v1/chat/completions", optionsHandler)
	router.OPTIONS("/v1/models", optionsHandler)
	router.OPTIONS("/v1/responses", optionsHandler)
	router.OPTIONS("/v1/images/generations", optionsHandler)
	router.OPTIONS("/v1/images/edits", optionsHandler)
	router.OPTIONS("/v1/images/variations", optionsHandler)
	router.OPTIONS("/v1/files", optionsHandler)
	router.OPTIONS("/v1/audio/speech", optionsHandler)
	router.OPTIONS("/v1/audio/transcriptions", optionsHandler)
	router.OPTIONS("/v1/audio/translations", optionsHandler)

	authGroup := router.Group("").Use(middlewares.Authorization)
	authGroup.POST("/v1/chat/completions", chatHandler.Nightmare)
	authGroup.POST("/v1/responses", chatHandler.Responses)
	authGroup.POST("/v1/files", chatHandler.Files)
	authGroup.GET("/v1/models", modelsHandler.ListModels)
	authGroup.POST("/backend-api/conversation", chatHandler.ChatGPTConversation)
	authGroup.POST("/v1/images/generations", imageHandler.Generations)
	authGroup.POST("/v1/images/edits", imageHandler.Edits)
	authGroup.POST("/v1/images/variations", imageHandler.Variations)
	authGroup.POST("/v1/audio/speech", audioHandler.TTS)
	authGroup.POST("/v1/audio/transcriptions", audioHandler.Transcriptions)
	authGroup.POST("/v1/audio/translations", audioHandler.Translations)

	return router
}
