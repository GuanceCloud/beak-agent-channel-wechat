package beak

import (
	"encoding/json"
	"fmt"
)

type Channel struct {
	ChannelUUID   string         `json:"channel_uuid"`
	WorkspaceUUID string         `json:"workspace_uuid"`
	Platform      string         `json:"platform"`
	Name          string         `json:"name"`
	Config        map[string]any `json:"config"`
	Status        string         `json:"status"`
}

type ListChannelsResponse struct {
	Channels []Channel `json:"channels"`
}

type CreateChannelRequest struct {
	WorkspaceUUID string         `json:"workspace_uuid"`
	Platform      string         `json:"platform"`
	Name          string         `json:"name,omitempty"`
	Config        map[string]any `json:"config"`
}

type CreateChannelResponse struct {
	ChannelUUID string `json:"channel_uuid"`
}

type Session struct {
	SessionUUID   string               `json:"session_uuid"`
	WorkspaceUUID string               `json:"workspace_uuid"`
	Platform      string               `json:"platform"`
	SessionType   string               `json:"session_type"`
	SourceType    string               `json:"source_type"`
	SourceID      string               `json:"source_id"`
	Status        string               `json:"status"`
	Participants  []SessionParticipant `json:"participants"`
}

type SessionParticipant struct {
	ParticipantID string `json:"participant_id"`
	Status        string `json:"status"`
}

type ListSessionsResponse struct {
	Sessions []Session `json:"sessions"`
}

type CreateSessionRequest struct {
	WorkspaceUUID  string         `json:"workspace_uuid"`
	Name           string         `json:"name,omitempty"`
	Platform       string         `json:"platform,omitempty"`
	SessionType    string         `json:"session_type,omitempty"`
	SourceType     string         `json:"source_type,omitempty"`
	SourceID       string         `json:"source_id,omitempty"`
	ParticipantIDs []string       `json:"participant_ids,omitempty"`
	Metadata       map[string]any `json:"metadata,omitempty"`
}

type CreateSessionResponse struct {
	SessionUUID   string               `json:"session_uuid"`
	WorkspaceUUID string               `json:"workspace_uuid"`
	Participants  []SessionParticipant `json:"participants"`
	Status        string               `json:"status"`
}

type AddParticipantsRequest struct {
	WorkspaceUUID  string   `json:"workspace_uuid"`
	ParticipantIDs []string `json:"participant_ids"`
	Reason         string   `json:"reason,omitempty"`
}

type AddParticipantsResponse struct {
	Participants []SessionParticipant `json:"participants"`
}

type CreateMessageRequest struct {
	WorkspaceUUID string         `json:"workspace_uuid"`
	SenderID      string         `json:"sender_id"`
	Content       string         `json:"content"`
	ReplyToUUID   string         `json:"reply_to_uuid,omitempty"`
	Metadata      map[string]any `json:"metadata,omitempty"`
}

type CreateMessageResponse struct {
	MessageUUID   string `json:"message_uuid"`
	WorkspaceUUID string `json:"workspace_uuid"`
	CreatedAt     string `json:"created_at"`
}

type StreamRequest struct {
	WorkspaceUUID string         `json:"workspace_uuid"`
	SubscriberID  string         `json:"subscriber_id"`
	LastEventUUID string         `json:"last_event_uuid,omitempty"`
	Filters       map[string]any `json:"filters,omitempty"`
}

type StreamChunk struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

type StreamEvent struct {
	EventUUID     string          `json:"event_uuid"`
	WorkspaceUUID string          `json:"workspace_uuid"`
	SessionUUID   string          `json:"session_uuid"`
	EventType     string          `json:"event_type"`
	MessageUUID   string          `json:"message_uuid,omitempty"`
	SenderID      string          `json:"sender_id,omitempty"`
	Content       string          `json:"content,omitempty"`
	Payload       json.RawMessage `json:"payload,omitempty"`
	Raw           json.RawMessage `json:"-"`
}

type MessageEventPayload struct {
	MessageUUID string `json:"message_uuid"`
	SenderID    string `json:"sender_id"`
	Content     string `json:"content"`
}

func DecodeStreamLine(line []byte) (*StreamEvent, error) {
	var chunk StreamChunk
	if err := json.Unmarshal(line, &chunk); err != nil {
		return nil, err
	}
	if chunk.Type == "" {
		var event StreamEvent
		if err := json.Unmarshal(line, &event); err != nil {
			return nil, err
		}
		event.Raw = append([]byte(nil), line...)
		if event.EventType == "" {
			event.EventType = event.EventTypeOrDefault()
		}
		return &event, nil
	}
	if chunk.Type == "heartbeat" {
		return &StreamEvent{EventType: "heartbeat", Payload: chunk.Payload, Raw: append([]byte(nil), line...)}, nil
	}
	if chunk.Type == "error" {
		return &StreamEvent{EventType: "error", Payload: chunk.Payload, Raw: append([]byte(nil), line...)}, nil
	}
	var event StreamEvent
	if len(chunk.Payload) > 0 {
		if err := json.Unmarshal(chunk.Payload, &event); err != nil {
			return nil, fmt.Errorf("decode stream payload: %w", err)
		}
		if len(event.Payload) == 0 {
			var payload MessageEventPayload
			if err := json.Unmarshal(chunk.Payload, &payload); err == nil && (payload.MessageUUID != "" || payload.SenderID != "" || payload.Content != "") {
				if event.MessageUUID == "" {
					event.MessageUUID = payload.MessageUUID
				}
				event.Payload = append([]byte(nil), chunk.Payload...)
			}
		}
	}
	if event.EventType == "" {
		event.EventType = chunk.Type
	}
	event.Raw = append([]byte(nil), line...)
	return &event, nil
}

func (e StreamEvent) MessagePayload() (MessageEventPayload, error) {
	payload := MessageEventPayload{
		MessageUUID: e.MessageUUID,
		SenderID:    e.SenderID,
		Content:     e.Content,
	}
	if len(e.Payload) == 0 {
		return payload, nil
	}
	var nested MessageEventPayload
	if err := json.Unmarshal(e.Payload, &nested); err != nil {
		return payload, err
	}
	if nested.MessageUUID != "" {
		payload.MessageUUID = nested.MessageUUID
	}
	if nested.SenderID != "" {
		payload.SenderID = nested.SenderID
	}
	if nested.Content != "" {
		payload.Content = nested.Content
	}
	return payload, nil
}

func (e StreamEvent) EventTypeOrDefault() string {
	if e.EventType != "" {
		return e.EventType
	}
	return "message"
}
