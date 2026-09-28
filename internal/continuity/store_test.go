package continuity

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	official "aurora/typings/official"
)

func request(messages ...official.APIMessage) official.APIRequest {
	return official.APIRequest{Model: "gpt-6-pro", Messages: messages}
}

func TestStateDirectoryRejectsSymlink(t *testing.T) {
	target := filepath.Join(t.TempDir(), "private")
	if err := os.Mkdir(target, 0700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(t.TempDir(), "state")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	req := request(official.NewTextMessage("user", "hello"))
	for _, dir := range []string{link, link + string(os.PathSeparator)} {
		if lease, err := New(dir).Prepare("gateway", "user", "chat", req); !errors.Is(err, ErrConflict) {
			if lease != nil {
				lease.Close()
			}
			t.Fatalf("symlink directory accepted: %v", err)
		}
	}
	lease, err := New(target).Prepare("gateway", "user", "chat", req)
	if err != nil {
		t.Fatalf("private real directory rejected: %v", err)
	}
	lease.Close()
	if err := os.Chmod(target, 0755); err != nil {
		t.Fatal(err)
	}
	if lease, err := New(target).Prepare("gateway", "user", "chat", req); !errors.Is(err, ErrConflict) {
		if lease != nil {
			lease.Close()
		}
		t.Fatalf("nonprivate directory accepted: %v", err)
	}
}

func prepare(t *testing.T, s *Store, user string, req official.APIRequest) *Lease {
	t.Helper()
	l, err := s.Prepare("gateway", user, "conversation", req)
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func complete(t *testing.T, l *Lease, conversation, parent string, output official.APIMessage) {
	t.Helper()
	if err := l.Start("account"); err != nil {
		t.Fatal(err)
	}
	if err := l.Commit("account", conversation, parent, output); err != nil {
		t.Fatal(err)
	}
	l.Close()
}

func TestScopePersistenceAndBranches(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")
	s := New(dir)
	system := official.NewTextMessage("system", "private instructions")
	user := official.NewTextMessage("user", "identical private prompt")
	answer := official.NewTextMessage("assistant", "private answer")
	first := request(system, user)
	complete(t, prepare(t, s, "alice", first), "upstream-alice", "parent-a", answer)
	bob := prepare(t, s, "bob", first)
	if bob.ConversationID != "" {
		t.Fatal("identical prompt merged users")
	}
	complete(t, bob, "upstream-bob", "parent-b", answer)

	s = New(dir) // No in-memory checkpoint survives this restart.
	next := request(system, user, answer, official.NewTextMessage("user", "second turn"))
	next.Model, next.ReasoningEffort = "gpt-6", "high"
	l := prepare(t, s, "alice", next)
	if l.ConversationID != "upstream-alice" || l.ParentID != "parent-a" ||
		len(l.Request.Messages) != 2 || l.Request.Messages[1].Text() != "second turn" {
		t.Fatalf("wrong resumed suffix: %+v", l)
	}
	answer2 := official.NewTextMessage("assistant", "second answer")
	complete(t, l, "upstream-alice", "parent-a2", answer2)

	branch := request(system, user, answer, official.NewTextMessage("user", "edited second turn"))
	l = prepare(t, s, "alice", branch)
	if l.ParentID != "parent-a" || l.ConversationID != "upstream-alice" {
		t.Fatal("branch did not select the proven earlier parent")
	}
	l.Close()
	for _, bad := range []official.APIRequest{
		first, next, request(system, official.NewTextMessage("user", "pruned")),
		request(system, user, official.NewTextMessage("assistant", "edited ancestor"), official.NewTextMessage("user", "new")),
		request(official.NewTextMessage("system", "changed"), user, answer, official.NewTextMessage("user", "new")),
	} {
		if _, err := s.Prepare("gateway", "alice", "conversation", bad); !errors.Is(err, ErrConflict) {
			t.Fatalf("unsafe history accepted: %v", err)
		}
	}
	if _, err := s.Prepare("gateway", "new-user", "old-chat", next); !errors.Is(err, ErrConflict) {
		t.Fatal("old conversation was silently backfilled")
	}
	otherGateway, err := s.Prepare("other-gateway", "alice", "conversation", first)
	if err != nil || otherGateway.ConversationID != "" {
		t.Fatal("gateway scopes merged")
	}
	otherGateway.Close()

	files, _ := filepath.Glob(filepath.Join(dir, "*.json"))
	for _, file := range files {
		data, _ := os.ReadFile(file)
		if strings.Contains(string(data), "private") || strings.Contains(string(data), "gateway") {
			t.Fatal("state contains user content/identity")
		}
		info, _ := os.Stat(file)
		if info.Mode().Perm() != 0600 {
			t.Fatal("state is not private")
		}
	}
}

func TestUnknownOutcomeAccountMismatchAndLock(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")
	s := New(dir)
	req := request(official.NewTextMessage("user", "hello"))
	l := prepare(t, s, "alice", req)
	if _, err := s.Prepare("gateway", "alice", "conversation", req); !errors.Is(err, ErrConflict) {
		t.Fatal("concurrent turn accepted")
	}
	if err := l.Start("account"); err != nil {
		t.Fatal(err)
	}
	if err := l.Commit("other-account", "conv", "parent", official.NewTextMessage("assistant", "answer")); !errors.Is(err, ErrConflict) {
		t.Fatal("account mismatch committed")
	}
	l.Close() // Simulates timeout/crash, not rollback.
	if _, err := New(dir).Prepare("gateway", "alice", "conversation", req); !errors.Is(err, ErrConflict) {
		t.Fatal("interrupted request retried after restart")
	}

	complete(t, prepare(t, s, "bob", req), "conv-b", "parent-b", official.NewTextMessage("assistant", "answer"))
	next := request(req.Messages[0], official.NewTextMessage("assistant", "answer"), official.NewTextMessage("user", "next"))
	l = prepare(t, s, "bob", next)
	defer l.Close()
	if err := l.Start("other-account"); !errors.Is(err, ErrConflict) {
		t.Fatal("continuation rebound to another account")
	}
}

func TestToolFollowupAndChangedDefinitions(t *testing.T) {
	s := New(filepath.Join(t.TempDir(), "state"))
	req := request(official.NewTextMessage("user", "use a tool"))
	req.Tools = []official.Tool{{Type: "function", Function: official.ToolFunction{Name: "lookup"}}}
	output := official.NewTextMessage("assistant", "")
	call := official.ToolCallRef{ID: "call-1", Type: "function"}
	call.Function.Name, call.Function.Arguments = "lookup", `{"b":2,"a":1}`
	output.ToolCalls = []official.ToolCallRef{call}
	complete(t, prepare(t, s, "alice", req), "conv", "tool-parent", output)
	output.ToolCalls[0].Index = 999 // Clients may omit or reconstruct streaming indexes.
	output.ToolCalls[0].Function.Arguments = `{ "a": 1, "b": 2 }`
	result := official.NewTextMessage("tool", "found")
	result.ToolCallID = "call-1"
	req.Messages = append(req.Messages, output, result)
	l := prepare(t, s, "alice", req)
	if l.ParentID != "tool-parent" || len(l.Request.Messages) != 1 || l.Request.Messages[0].Role != "tool" ||
		l.Request.Messages[0].Name != "lookup" {
		t.Fatal("tool result was not resumed as the new suffix")
	}
	l.Close()
	req.Tools[0].Function.Description = "new image IDs available"
	if _, err := s.Prepare("gateway", "alice", "conversation", req); !errors.Is(err, ErrConflict) {
		t.Fatal("changed toolkit context silently accepted")
	}
	req.Tools[0].Function.Description = ""
	req.Messages[2].ToolCallID = "wrong-call"
	if _, err := s.Prepare("gateway", "alice", "conversation", req); !errors.Is(err, ErrConflict) {
		t.Fatal("unknown tool result accepted")
	}
}

func TestConfigurationAndCorruptStateFailClosed(t *testing.T) {
	req := request(official.NewTextMessage("user", "hello"))
	for _, scope := range [][4]string{{"", "gateway", "user", "conv"}, {t.TempDir(), "", "user", "conv"}, {t.TempDir(), "gateway", "", "conv"}} {
		if _, err := New(scope[0]).Prepare(scope[1], scope[2], scope[3], req); !errors.Is(err, ErrConflict) {
			t.Fatal("partial configuration accepted")
		}
	}
	s := New(filepath.Join(t.TempDir(), "state"))
	l := prepare(t, s, "alice", req)
	path := l.path
	l.Close()
	if err := os.WriteFile(path, []byte("{truncated"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Prepare("gateway", "alice", "conversation", req); !errors.Is(err, ErrConflict) {
		t.Fatal("corrupt state created a new thread")
	}
}

func TestAmbiguousCheckpointFailsClosed(t *testing.T) {
	s := New(filepath.Join(t.TempDir(), "state"))
	user := official.NewTextMessage("user", "hello")
	answer := official.NewTextMessage("assistant", "answer")
	l := prepare(t, s, "alice", request(user))
	complete(t, l, "conv", "parent-1", answer)
	duplicate := l.record.Checkpoints[0]
	duplicate.Parent = "parent-2"
	l.record.Checkpoints = append(l.record.Checkpoints, duplicate)
	if err := s.write(l.path, l.record); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Prepare("gateway", "alice", "conversation", request(user, answer, official.NewTextMessage("user", "next"))); !errors.Is(err, ErrConflict) {
		t.Fatal("ambiguous parent was guessed")
	}
}

func TestParallelToolResultsKeepCallOrderAndNames(t *testing.T) {
	s := New(filepath.Join(t.TempDir(), "state"))
	user := official.NewTextMessage("user", "use tools")
	output := official.NewTextMessage("assistant", "")
	for _, id := range []string{"one", "two"} {
		call := official.ToolCallRef{ID: id, Type: "function"}
		call.Function.Name, call.Function.Arguments = "lookup_"+id, `{}`
		output.ToolCalls = append(output.ToolCalls, call)
	}
	complete(t, prepare(t, s, "alice", request(user)), "conv", "parent", output)
	two := official.NewTextMessage("tool", "second result")
	two.ToolCallID = "two"
	one := official.NewTextMessage("tool", "first result")
	one.ToolCallID = "one"
	l := prepare(t, s, "alice", request(user, output, two, one))
	defer l.Close()
	if len(l.Request.Messages) != 2 || l.Request.Messages[0].ToolCallID != "one" ||
		l.Request.Messages[0].Name != "lookup_one" || l.Request.Messages[1].ToolCallID != "two" ||
		l.Request.Messages[1].Name != "lookup_two" {
		t.Fatalf("tool results lost their associations: %+v", l.Request.Messages)
	}
}
