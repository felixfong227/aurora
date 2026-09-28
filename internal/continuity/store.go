// Package continuity owns opt-in Chat Completions checkpoints. It never discovers
// conversations from prompts: every lookup requires an authenticated, user-scoped key.
package continuity

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	official "aurora/typings/official"
)

var ErrConflict = errors.New("conversation continuity conflict")

func conflict(reason string) error { return fmt.Errorf("%w: %s", ErrConflict, reason) }

func hash(v any) string {
	b, _ := json.Marshal(v)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

type checkpoint struct {
	History string
	Count   int
	Parent  string
	Request string
}

type record struct {
	Version      int
	Account      string
	Conversation string
	Settings     string
	Inflight     bool
	Checkpoints  []checkpoint
}

type Store struct {
	dir   string
	locks sync.Map
}

func New(dir string) *Store { return &Store{dir: dir} }

// Lease holds the conversation lock until Close. Prepare does not write anything;
// Start must durably mark an unknown outcome BEFORE any upstream work begins.
type Lease struct {
	store   *Store
	path    string
	unlock  func()
	record  record
	history []string
	request string
	started bool

	Account        string
	ConversationID string
	ParentID       string
	Request        official.APIRequest
}

func (l *Lease) Close() {
	if l != nil && l.unlock != nil {
		l.unlock()
		l.unlock = nil
	}
}

// Prepare supports complete histories ending in one new user message or a complete
// batch of tool results. Pruned histories and retries are deliberately conflicts.
func (s *Store) Prepare(gateway, user, conversation string, req official.APIRequest) (*Lease, error) {
	if s.dir == "" || gateway == "" || strings.TrimSpace(user) == "" || strings.TrimSpace(conversation) == "" {
		return nil, conflict("state directory and authenticated gateway/user/conversation are required")
	}
	key := hash([]string{gateway, user, conversation})
	value, _ := s.locks.LoadOrStore(key, &sync.Mutex{})
	lock := value.(*sync.Mutex)
	if !lock.TryLock() {
		return nil, conflict("a turn is already running")
	}
	l := &Lease{store: s, path: filepath.Join(s.dir, key+".json"), unlock: lock.Unlock}
	ok := false
	defer func() {
		if !ok {
			l.Close()
		}
	}()
	if err := os.MkdirAll(s.dir, 0700); err != nil {
		return nil, err
	}
	// Reject a symlink at the configured directory, not same-UID path races.
	info, err := os.Lstat(filepath.Clean(s.dir))
	if err != nil || !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return nil, conflict("state directory must be a private directory (0700), not a symlink")
	}
	if info, err := os.Lstat(l.path); err == nil {
		if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
			return nil, conflict("state file must be a private regular file")
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	data, err := os.ReadFile(l.path)
	exists := err == nil
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	if exists {
		if err := json.Unmarshal(data, &l.record); err != nil || l.record.Version != 1 ||
			l.record.Account == "" || l.record.Settings == "" {
			return nil, conflict("invalid persistent state; operator review required")
		}
		if l.record.Inflight {
			return nil, conflict("previous turn outcome is unknown; operator review required")
		}
		if l.record.Conversation == "" || len(l.record.Checkpoints) == 0 {
			return nil, conflict("persistent state has no proven checkpoint")
		}
	}
	var instructions, messages []official.APIMessage
	for _, m := range req.Messages {
		if m.Role == "system" {
			if len(messages) != 0 {
				return nil, conflict("system instructions must precede the history")
			}
			instructions = append(instructions, m)
		} else {
			messages = append(messages, m)
		}
	}
	for _, m := range req.Messages {
		if m.Role != "system" && m.Role != "user" && m.Role != "assistant" && m.Role != "tool" {
			return nil, conflict("unsupported message role")
		}
		if len(m.Files()) != 0 || len(m.Attachments) != 0 {
			return nil, conflict("file/image inputs are not supported by continuity")
		}
		for _, part := range m.Content.Parts {
			if part.Type != "" && part.Type != "text" && part.Type != "input_text" && part.Type != "output_text" {
				return nil, conflict("only text message content is supported")
			}
		}
	}
	settings := req
	settings.Messages = instructions
	settings.Stream, settings.StreamOptions = false, nil
	settings.User, settings.Metadata, settings.Store = "", nil, nil
	// These are per-POST controls, not instructions retained in the thread.
	settings.Model, settings.ReasoningEffort = "", ""
	settings.Temperature, settings.TopP = nil, nil
	settings.N, settings.Seed, settings.LogitBias = nil, nil, nil
	settings.PresencePenalty, settings.FrequencyPenalty = nil, nil
	settingsHash := hash(settings)
	if exists && l.record.Settings != settingsHash {
		return nil, conflict("instructions, tools or prompt-injected settings changed")
	}
	for _, m := range messages {
		fingerprint, err := messageHash(m)
		if err != nil {
			return nil, err
		}
		l.history = append(l.history, fingerprint)
	}
	l.request = hash(l.history)
	start := 0
	if exists {
		matches := 0
		for _, cp := range l.record.Checkpoints {
			if cp.Request == l.request {
				return nil, conflict("request already completed; retry/regeneration is not supported")
			}
			if cp.Count > 0 && cp.Count < len(l.history) && hash(l.history[:cp.Count]) == cp.History {
				// A full prefix proves the branch, not a matching last prompt.
				if cp.Count > start {
					start, matches, l.ParentID = cp.Count, 1, cp.Parent
				} else if cp.Count == start {
					matches++
				}
			}
		}
		if matches != 1 || l.ParentID == "" {
			return nil, conflict("history has no unambiguous committed parent; pruned histories are unsupported")
		}
		l.Account, l.ConversationID = l.record.Account, l.record.Conversation
	}
	suffix := messages[start:]
	if err := validateSuffix(messages, start); err != nil {
		return nil, err
	}
	if suffix[0].Role == "tool" {
		// The upstream emulation protocol has names/order, not OpenAI call IDs.
		// Send results in call order and restore optional names from the proven
		// assistant message, so parallel results cannot be silently misassociated.
		ordered := make([]official.APIMessage, 0, len(suffix))
		for _, call := range messages[start-1].ToolCalls {
			for _, result := range suffix {
				if result.ToolCallID == call.ID {
					result.Name = call.Function.Name
					ordered = append(ordered, result)
				}
			}
		}
		suffix = ordered
	}
	l.Request = req
	l.Request.Messages = append(append([]official.APIMessage(nil), instructions...), suffix...)
	if !exists {
		l.record = record{Version: 1, Settings: settingsHash}
	}
	ok = true
	return l, nil
}

func validateSuffix(messages []official.APIMessage, start int) error {
	suffix := messages[start:]
	if len(suffix) == 1 && suffix[0].Role == "user" && len(suffix[0].ToolCalls) == 0 {
		if start > 0 && len(messages[start-1].ToolCalls) > 0 {
			return conflict("pending tool calls require their results")
		}
		return nil
	}
	if start == 0 || messages[start-1].Role != "assistant" {
		return conflict("start a fresh conversation with exactly one user message; old histories cannot be backfilled")
	}
	calls := messages[start-1].ToolCalls
	if len(calls) == 0 || len(calls) != len(suffix) {
		return conflict("expected exactly one result for every pending tool call")
	}
	seen := map[string]bool{}
	for _, m := range suffix {
		i := slices.IndexFunc(calls, func(c official.ToolCallRef) bool { return c.ID == m.ToolCallID })
		if m.Role != "tool" || i < 0 || seen[m.ToolCallID] || (m.Name != "" && m.Name != calls[i].Function.Name) {
			return conflict("tool result does not match pending tool calls")
		}
		seen[m.ToolCallID] = true
	}
	return nil
}

// messageHash normalizes only equivalent visible API forms: text parts, tool-call
// indexes and JSON argument whitespace/key order. IDs and content remain significant.
func messageHash(m official.APIMessage) (string, error) {
	m.Content = official.MessageContent{TextValue: m.Text()}
	for i := range m.ToolCalls {
		// Copy before normalizing; callers still own their request.
		if i == 0 {
			m.ToolCalls = slices.Clone(m.ToolCalls)
		}
		m.ToolCalls[i].Index = i
		var args any
		if !json.Valid([]byte(m.ToolCalls[i].Function.Arguments)) {
			return "", conflict("invalid tool-call arguments")
		}
		decoder := json.NewDecoder(strings.NewReader(m.ToolCalls[i].Function.Arguments))
		decoder.UseNumber()
		if err := decoder.Decode(&args); err != nil {
			return "", conflict("invalid tool-call arguments")
		}
		b, err := json.Marshal(args)
		if err != nil {
			return "", err
		}
		m.ToolCalls[i].Function.Arguments = string(b)
	}
	return hash(m), nil
}

func (l *Lease) Start(account string) error {
	if l.unlock == nil || l.started || account == "" || (l.Account != "" && l.Account != account) {
		return conflict("upstream account binding mismatch")
	}
	l.record.Account, l.record.Inflight = account, true
	if err := l.store.write(l.path, l.record); err != nil {
		return err
	}
	l.Account, l.started = account, true
	return nil
}

// Commit accepts only a proven completed upstream turn and the exact visible API
// assistant message, not the raw emulated tool transcript before normalization.
func (l *Lease) Commit(account, conversation, parent string, output official.APIMessage) error {
	if l.unlock == nil || !l.started || account != l.Account || conversation == "" || parent == "" || output.Role != "assistant" ||
		(l.ConversationID != "" && conversation != l.ConversationID) {
		return conflict("upstream result does not match the active conversation")
	}
	fingerprint, err := messageHash(output)
	if err != nil {
		return err
	}
	history := append(slices.Clone(l.history), fingerprint)
	l.record.Conversation = conversation
	l.record.Checkpoints = append(l.record.Checkpoints, checkpoint{
		History: hash(history), Count: len(history), Parent: parent, Request: l.request,
	})
	l.record.Inflight = false
	if err := l.store.write(l.path, l.record); err != nil {
		return err
	}
	l.started = false
	return nil
}

func (s *Store) write(path string, value record) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(s.dir, ".turn-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(data); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err := os.Rename(f.Name(), path); err != nil {
		return err
	}
	dir, err := os.Open(s.dir)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
