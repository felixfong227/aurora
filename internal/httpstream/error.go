package httpstream

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// WriteChatCompletionError terminates a failed turn without stop or [DONE].
// Staged SSE headers do not commit a response; those failures still get JSON.
func WriteChatCompletionError(c *gin.Context, err error) {
	payload := gin.H{"error": gin.H{
		"message": err.Error(),
		"type":    "server_error",
		"param":   nil,
		"code":    "upstream_error",
	}}
	if c.Writer.Written() {
		WriteSSEEvent(c, "", payload)
	} else {
		c.Header("Content-Type", "application/json; charset=utf-8")
		c.JSON(http.StatusBadGateway, payload)
	}
	c.Abort()
}
