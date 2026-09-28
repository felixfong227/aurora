# LibreChat image delivery

This fork fixes two failures in the ChatGPT-backed chat-completions path:

- A stream handoff could end with HTTP 200 and an empty answer. For an empty
  incomplete handoff, Aurora now reads the existing upstream conversation until
  the submitted user turn completes. It does not submit another generation.
  Failed recovery returns an explicit error.
- Generated images could appear only in custom metadata that LibreChat ignores.
  The final image now also appears as ordinary Markdown before the response ends.

## Images without local file storage

Use a versioned image from `ghcr.io/felixfong227/aurora` and add this to the
Aurora service's Compose `environment`:

```yaml
IMAGE_PROXY_PUBLIC_URL: https://aurora.example
```

Use the URL that **users' browsers** can reach, not a Docker-internal hostname.
Keep the service's existing `Authorization` secret, at least 32 characters,
and its configured access-token file. Never put a real secret in this repository.

Aurora returns signed links to individual generated images. When a browser opens
one, Aurora retrieves a fresh OpenAI download URL, authenticates with the correct
configured account, and streams the image. There are no local image files,
base64 image copies in chat messages, or image caches. The browser receives
`Cache-Control: no-store`.

Treat the complete image URL as private: anyone holding it can view that image.
Links survive restarts and token renewal. Rotating `Authorization`, removing the
account, or deleting the upstream image revokes access. If an external reverse
proxy logs requests, exclude `/image-proxy/` there too; Aurora skips those logs.

The relay accepts raster images from the configured backend's exact
`/estuary/content` endpoint. It rejects redirects, other origins, HTML and SVG.
It supports configured accounts, not temporary externally supplied tokens.

## Limits

- This does not replace LibreChat's upload storage or synchronize ChatGPT Library.
- Existing blank messages are not retroactively repaired.
- It does not map successive LibreChat turns to the same ChatGPT conversation.
- Partial answers and `continue` handoffs fail explicitly rather than replaying
  content already delivered.
- No model routing or usage/billing policy is changed.

## Publishing

The Docker workflow publishes `image-*` tags to this fork's GHCR namespace.
Unlike `v*`, these tags do not trigger the separate binary-release workflow.
An image tag such as `image-librechat-20260928.1` produces a
`librechat-20260928.1` container tag and a 12-character source-commit tag.
