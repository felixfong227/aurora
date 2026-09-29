# LibreChat image delivery

This fork fixes two failures in the ChatGPT-backed chat-completions path:

- A stream handoff could end with HTTP 200 and an empty answer. For an empty
  incomplete handoff, Aurora now reads the existing upstream conversation until
  the submitted user turn completes. It does not submit another generation.
  Failed recovery returns an explicit error.
- Generated images could appear only in custom metadata that LibreChat ignores.
  The final image now also appears as ordinary Markdown before the response ends.

## Pro models, reasoning and sources

Explicit model IDs stay selected when reasoning effort changes, including the
advertised thinking models. A `reason` hint does not replace the selected model
with `auto`. If no effort is supplied, Aurora leaves it unset rather than forcing
`standard`, so the backend chooses the model's default.

For `gpt-5-6-thinking`, API effort levels `medium`, `high`, and `xhigh` map to
ChatGPT's `standard`, `extended`, and `max` respectively. Those are distinct
Standard, Extended, and Heavy choices in the website model catalog. Both request
conversion and final serialization use the same mapping. This translates API
names; it does not guarantee that every model supports every effort level.

Background-turn recovery allows up to ten minutes and sends SSE keep-alive
comments while waiting. Public thought summaries, reasoning recap labels and
observed web-search activity are forwarded as reasoning content. Detailed private
thought content is not forwarded by this summary path. Complete responses and
recovered snapshots retain the same citation links as streamed reference patches.

This does not force a web search or guarantee identical website answers. It
preserves the settings and user-visible information available from the backend.

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

## Opt-in conversation continuity

Aurora can map one fresh LibreChat conversation to one upstream ChatGPT thread.
It requires persistent metadata storage and both trusted identity headers on
every Chat Completions request. It never finds a conversation from a prompt.

Set `CONVERSATION_STATE_DIR` to a persistent directory private to Aurora. The
directory must be writable by the container's nonroot UID, with mode `0700`.
Aurora creates it if its parent is writable; checkpoint files use mode `0600`.
For example, mount a named volume at the image's nonroot-owned `/home/nonroot`
and set `CONVERSATION_STATE_DIR=/home/nonroot/conversations`. Verify ownership
for your image instead of assuming `/data` is writable. Use one Aurora process
per directory, not replicas sharing the volume.

The existing `ENABLE_HISTORY` setting is unchanged. These checkpoints do not
override upstream temporary-chat retention or make temporary chats appear in
ChatGPT's history. An expired/deleted upstream thread fails rather than being
replaced with a new one.

Configure LibreChat's built-in OpenAI endpoint with server-resolved headers:

```yaml
endpoints:
  openAI:
    titleConvo: false
    headers:
      X-Aurora-User-Id: "{{LIBRECHAT_USER_ID}}"
      X-Aurora-Conversation-Id: "{{LIBRECHAT_BODY_CONVERSATIONID}}"
```

Keep the existing gateway `Authorization` credential. Do not expose that
credential to untrusted callers who could forge another user's headers. The
gateway must supply the authenticated user ID, not accept it from browser input.
Separate title generation must remain disabled; titles can be set manually.
`BODY_MESSAGEID` and `BODY_PARENTMESSAGEID` are not used as upstream identities.

### Supported turns

- Start a **new** LibreChat conversation with one user message and optional
  leading `system` instructions. Conversations created before enabling this
  feature have no mapping and cannot be backfilled from their transcript.
- Subsequent requests must include the complete visible branch history, followed
  by one new user message or one result for each pending tool call.
- Aurora matches hashes only within the authenticated gateway/user/conversation
  scope. It submits only the new suffix with the recorded upstream conversation
  and assistant parent IDs. Fixed instructions are not appended again.
- An edit of the newest user message after a proven assistant checkpoint can
  form a branch inside the same upstream conversation. The entire prefix through
  that assistant must match. A matching last prompt alone is never sufficient.
- Fixed-definition agent tools work with JSON and SSE responses. Emulated tool
  calls have canonical empty assistant content plus `tool_calls`, so raw
  `<tool_call>` markup is not replayed. Tool IDs remain significant; argument
  JSON whitespace/key order and streaming indexes are normalized.
- Model, reasoning effort and sampling controls may change without changing the
  upstream thread. Existing model translation behavior is unchanged.
- Checkpoints and stable account binding survive restart and access-token
  renewal. Continuation uses the same configured account's TLS client; removing
  or disabling that account returns a conflict, never a random replacement.

### Deliberate conflicts and limits

Missing one header, empty/duplicate headers, absent state configuration, unknown
history, changed retained instructions or unavailable account return HTTP 409.
Requests with neither header remain stateless. Do not remove both headers from
an active integration as a workaround for a conflict.

This version rejects pruned histories, changed ancestors, first-turn regeneration,
retries of completed requests, ambiguous checkpoints, file/image inputs and
unsupported message roles such as `developer`. It does not promise recovery of
an old conversation after state loss. Keep the state volume and gateway credential
stable; rotating the credential changes its scope.

System instructions, tool definitions/descriptions, `tool_choice`, and controls
the converter embeds in the prompt, such as `max_tokens`, `stop` and
`response_format`, must remain fixed. Changes return a conflict rather than
silently keeping old instructions. In particular, LibreChat Image Creator
followups that change tool context/descriptions when image IDs become available
are not supported. Fixed-definition image-tool results can continue the chat,
but native `/images/generations` jobs still create separate upstream image jobs.
This does **not** make native image generation/editing part of the chat thread.
The Responses API's `previous_response_id` behavior is unchanged.

Aurora durably marks a turn in flight before contacting upstream. Timeout,
disconnect, missing terminal event, length-limited partial completion or failed
commit leaves the outcome blocked, including after restart. Internal tool-refusal
retries are disabled for opted-in turns. An operator must inspect an unknown
outcome; do not delete its state and blindly retry. There is no automatic
reconciliation or cached-response replay. Streaming may already have delivered
partial text before the error; no checkpoint is committed for that partial text.

State files contain only hashes, upstream IDs, account identity hashes and an
in-flight flag, not credentials, images or message bodies. Hashes are still
sensitive metadata. Back up the volume privately. Checkpoints have no automatic
expiry; storage grows with conversations/turns. Retire state only when the
corresponding conversations will no longer be used.

## Limits

- This does not replace LibreChat's upload storage or synchronize ChatGPT Library.
- Existing blank messages are not retroactively repaired.
- Stateless requests and native image jobs do not share a ChatGPT conversation.
- Partial answers and `continue` handoffs fail explicitly rather than replaying
  content already delivered.
- No usage/billing policy is enforced by these compatibility fixes.

## Publishing

The Docker workflow publishes `image-*` tags to this fork's GHCR namespace.
Unlike `v*`, these tags do not trigger the separate binary-release workflow.
An image tag such as `image-librechat-20260928.1` produces a
`librechat-20260928.1` container tag and a 12-character source-commit tag.
