package beak

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
)

func TestEnsurePeerSessionCreatesOnlySession(t *testing.T) {
	var createBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("token") != "" {
			t.Fatalf("token must not be sent in query: %s", r.URL.RawQuery)
		}
		if r.Header.Get("Authorization") != "Bearer beak-token" {
			t.Fatalf("authorization header=%q", r.Header.Get("Authorization"))
		}
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/sessions":
			if r.URL.Query().Get("source_type") != SourceTypeWeixinPeer {
				t.Fatalf("source_type=%q", r.URL.Query().Get("source_type"))
			}
			_ = json.NewEncoder(w).Encode(ListSessionsResponse{Sessions: nil})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/sessions":
			if err := json.NewDecoder(r.Body).Decode(&createBody); err != nil {
				t.Fatal(err)
			}
			if _, ok := createBody["task"]; ok {
				t.Fatal("session request must not create a task")
			}
			participants, ok := createBody["participant_ids"].([]any)
			if !ok {
				t.Fatalf("participant_ids missing: %+v", createBody)
			}
			var ids []string
			for _, participant := range participants {
				ids = append(ids, participant.(string))
			}
			for _, want := range []string{"user:weixin:peer-1", "agent:agent-1", "bridge:weixin"} {
				if !slices.Contains(ids, want) {
					t.Fatalf("missing participant %q in %+v", want, ids)
				}
			}
			_ = json.NewEncoder(w).Encode(CreateSessionResponse{SessionUUID: "sess-1"})
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	client := NewClient(server.URL, "beak-token")
	sessionUUID, err := client.EnsurePeerSession(context.Background(), "workspace-1", "account-1", "peer-1", "agent:agent-1", "bridge:weixin")
	if err != nil {
		t.Fatal(err)
	}
	if sessionUUID != "sess-1" {
		t.Fatalf("sessionUUID=%q", sessionUUID)
	}
	if createBody["source_id"] != "account-1:peer-1" {
		t.Fatalf("source_id=%v", createBody["source_id"])
	}
}

func TestEnsureChatSessionCreatesIMChatSession(t *testing.T) {
	var createBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/sessions":
			if r.URL.Query().Get("source_type") != SourceTypeIMChat {
				t.Fatalf("source_type=%q", r.URL.Query().Get("source_type"))
			}
			if r.URL.Query().Get("source_id") != "weixin:account-1:group:group-1" {
				t.Fatalf("source_id=%q", r.URL.Query().Get("source_id"))
			}
			_ = json.NewEncoder(w).Encode(ListSessionsResponse{Sessions: nil})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/sessions":
			if err := json.NewDecoder(r.Body).Decode(&createBody); err != nil {
				t.Fatal(err)
			}
			if _, ok := createBody["task"]; ok {
				t.Fatal("chat session request must not create a task")
			}
			if createBody["source_type"] != SourceTypeIMChat || createBody["source_id"] != "weixin:account-1:group:group-1" {
				t.Fatalf("create body=%+v", createBody)
			}
			participants, ok := createBody["participant_ids"].([]any)
			if !ok {
				t.Fatalf("participant_ids missing: %+v", createBody)
			}
			var ids []string
			for _, participant := range participants {
				ids = append(ids, participant.(string))
			}
			for _, want := range []string{"im:weixin:group:group-1:user:user-1", "agent:agent-1", "bridge:weixin"} {
				if !slices.Contains(ids, want) {
					t.Fatalf("missing participant %q in %+v", want, ids)
				}
			}
			_ = json.NewEncoder(w).Encode(CreateSessionResponse{SessionUUID: "sess-1"})
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	client := NewClient(server.URL, "beak-token")
	sessionUUID, err := client.EnsureChatSession(context.Background(), "workspace-1", "account-1", "group", "group-1", "user-1", "agent:agent-1", "bridge:weixin")
	if err != nil {
		t.Fatal(err)
	}
	if sessionUUID != "sess-1" {
		t.Fatalf("sessionUUID=%q", sessionUUID)
	}
}

func TestEnsureChatSessionSeparatesSameGroupByAccount(t *testing.T) {
	var listedSourceIDs []string
	var createdSourceIDs []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/sessions":
			listedSourceIDs = append(listedSourceIDs, r.URL.Query().Get("source_id"))
			_ = json.NewEncoder(w).Encode(ListSessionsResponse{Sessions: nil})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/sessions":
			var createBody map[string]any
			if err := json.NewDecoder(r.Body).Decode(&createBody); err != nil {
				t.Fatal(err)
			}
			createdSourceIDs = append(createdSourceIDs, createBody["source_id"].(string))
			_ = json.NewEncoder(w).Encode(CreateSessionResponse{SessionUUID: "sess-" + createBody["source_id"].(string)})
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	client := NewClient(server.URL, "beak-token")
	if _, err := client.EnsureChatSession(context.Background(), "workspace-1", "account-1", "group", "group-1", "user-1", "agent:agent-1", "bridge:weixin"); err != nil {
		t.Fatal(err)
	}
	if _, err := client.EnsureChatSession(context.Background(), "workspace-1", "account-2", "group", "group-1", "user-1", "agent:agent-1", "bridge:weixin"); err != nil {
		t.Fatal(err)
	}

	want := []string{"weixin:account-1:group:group-1", "weixin:account-2:group:group-1"}
	if !slices.Equal(listedSourceIDs, want) {
		t.Fatalf("listed source ids=%+v", listedSourceIDs)
	}
	if !slices.Equal(createdSourceIDs, want) {
		t.Fatalf("created source ids=%+v", createdSourceIDs)
	}
}

func TestStreamEventsPostsNDJSONRequest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/v1/sessions/sess-1/stream" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		var body StreamRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body.WorkspaceUUID != "workspace-1" || body.SubscriberID != "bridge:weixin" || body.LastEventUUID != "evt-0" {
			t.Fatalf("unexpected stream body: %+v", body)
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = w.Write([]byte(`{"type":"message","payload":{"event_uuid":"evt-1","workspace_uuid":"workspace-1","session_uuid":"sess-1","event_type":"message","payload":{"message_uuid":"msg-1","sender_id":"agent:agent-1","content":"hello"}}}` + "\n"))
		_, _ = w.Write([]byte(`{"type":"heartbeat","payload":{"event_type":"heartbeat"}}` + "\n"))
	}))
	defer server.Close()

	client := NewClient(server.URL, "beak-token")
	var events []StreamEvent
	err := client.StreamEvents(context.Background(), "sess-1", StreamRequest{
		WorkspaceUUID: "workspace-1",
		SubscriberID:  "bridge:weixin",
		LastEventUUID: "evt-0",
	}, func(event StreamEvent) error {
		events = append(events, event)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 {
		t.Fatalf("events=%d", len(events))
	}
	payload, err := events[0].MessagePayload()
	if err != nil {
		t.Fatal(err)
	}
	if payload.SenderID != "agent:agent-1" || payload.Content != "hello" {
		t.Fatalf("payload=%+v", payload)
	}
	if events[1].EventType != "heartbeat" {
		t.Fatalf("second event=%+v", events[1])
	}
}

func TestEnsureChannelLinkSessionCreatesOnlySession(t *testing.T) {
	var createBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/sessions":
			if r.URL.Query().Get("source_type") != SourceTypeWeixinChannelLink {
				t.Fatalf("source_type=%q", r.URL.Query().Get("source_type"))
			}
			if r.URL.Query().Get("source_id") != "account-1" {
				t.Fatalf("source_id=%q", r.URL.Query().Get("source_id"))
			}
			_ = json.NewEncoder(w).Encode(ListSessionsResponse{Sessions: nil})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/sessions":
			if err := json.NewDecoder(r.Body).Decode(&createBody); err != nil {
				t.Fatal(err)
			}
			if _, ok := createBody["task"]; ok {
				t.Fatal("channel link session request must not create a task")
			}
			if createBody["session_type"] != "channel_link" {
				t.Fatalf("session_type=%v", createBody["session_type"])
			}
			if createBody["source_type"] != SourceTypeWeixinChannelLink {
				t.Fatalf("source_type=%v", createBody["source_type"])
			}
			participants, ok := createBody["participant_ids"].([]any)
			if !ok {
				t.Fatalf("participant_ids missing: %+v", createBody)
			}
			var ids []string
			for _, participant := range participants {
				ids = append(ids, participant.(string))
			}
			for _, want := range []string{"agent:agent-1", "bridge:weixin"} {
				if !slices.Contains(ids, want) {
					t.Fatalf("missing participant %q in %+v", want, ids)
				}
			}
			_ = json.NewEncoder(w).Encode(CreateSessionResponse{SessionUUID: "channel-link-sess-1"})
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	client := NewClient(server.URL, "beak-token")
	sessionUUID, err := client.EnsureChannelLinkSession(context.Background(), "workspace-1", "account-1", "agent:agent-1", "bridge:weixin")
	if err != nil {
		t.Fatal(err)
	}
	if sessionUUID != "channel-link-sess-1" {
		t.Fatalf("sessionUUID=%q", sessionUUID)
	}
}

func TestDecodeStreamLineAcceptsDirectMessagePayload(t *testing.T) {
	event, err := DecodeStreamLine([]byte(`{"type":"message","payload":{"message_uuid":"msg-1","sender_id":"agent:agent-1","content":"hello"}}`))
	if err != nil {
		t.Fatal(err)
	}
	payload, err := event.MessagePayload()
	if err != nil {
		t.Fatal(err)
	}
	if event.EventType != "message" {
		t.Fatalf("event type=%q", event.EventType)
	}
	if payload.MessageUUID != "msg-1" || payload.SenderID != "agent:agent-1" || payload.Content != "hello" {
		t.Fatalf("payload=%+v", payload)
	}
}
