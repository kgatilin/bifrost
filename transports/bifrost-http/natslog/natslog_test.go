package natslog

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/maximhq/bifrost/core/schemas"
)

type recorder struct {
	subject string
	events  []Event
}

func (r *recorder) Publish(_ context.Context, subject string, data []byte) error {
	r.subject = subject
	var ev Event
	if err := json.Unmarshal(data, &ev); err != nil {
		return err
	}
	r.events = append(r.events, ev)
	return nil
}

func TestInjectPublishesTheLastAttempt(t *testing.T) {
	start := time.Date(2026, 9, 28, 10, 0, 0, 0, time.UTC)
	trace := &schemas.Trace{
		RequestID: "req-1",
		StartTime: start,
		EndTime:   start.Add(1500 * time.Millisecond),
		Spans: []*schemas.Span{
			{Kind: schemas.SpanKindLLMCall, EndTime: start.Add(time.Second), Status: schemas.SpanStatusError, StatusMsg: "429",
				Attributes: map[string]any{schemas.AttrProviderName: "vertex", schemas.AttrRequestModel: "gemini-3.5-flash"}},
			{Kind: schemas.SpanKindRetry, EndTime: start.Add(1400 * time.Millisecond),
				Attributes: map[string]any{
					schemas.AttrProviderName: "vertex", schemas.AttrRequestModel: "gemini-3.5-flash", schemas.AttrOperationName: "chat_completion",
					schemas.AttrInputTokens: 100, schemas.AttrOutputTokens: int64(20), schemas.AttrTotalTokens: 120,
					schemas.AttrUsageCost: 0.0012,
				}},
			{Kind: schemas.SpanKindPlugin, EndTime: start.Add(1450 * time.Millisecond)},
		},
		RequestHeaders: map[string]string{"x-bf-lh-app": "indexer", "x-bf-lh-purpose": "extract", "user-agent": "go"},
	}
	r := &recorder{}
	p := New(Config{}, r, nil)
	if err := p.Inject(context.Background(), trace); err != nil {
		t.Fatal(err)
	}
	if r.subject != "evt.llm.call" || len(r.events) != 1 {
		t.Fatalf("subject %q, %d events", r.subject, len(r.events))
	}
	ev := r.events[0]
	if ev.Status != "ok" || ev.Provider != "vertex" || ev.Model != "gemini-3.5-flash" || ev.RequestType != "chat_completion" {
		t.Errorf("identity %+v", ev)
	}
	if ev.InputTokens != 100 || ev.OutputTokens != 20 || ev.TotalTokens != 120 || ev.CostUSD != 0.0012 {
		t.Errorf("usage %+v", ev)
	}
	if ev.LatencyMs != 1500 || ev.RequestID != "req-1" {
		t.Errorf("latency %d, request %q", ev.LatencyMs, ev.RequestID)
	}
	if len(ev.Headers) != 2 || ev.Headers["app"] != "indexer" || ev.Headers["purpose"] != "extract" {
		t.Errorf("headers %v", ev.Headers)
	}
}

func TestInjectReportsAnError(t *testing.T) {
	trace := &schemas.Trace{Spans: []*schemas.Span{{
		Kind: schemas.SpanKindLLMCall, Status: schemas.SpanStatusError, StatusMsg: "boom",
		Attributes: map[string]any{
			schemas.AttrProviderName: "ollama", schemas.AttrRequestModel: "qwen3",
			schemas.AttrInputTokens: 7, schemas.AttrOutputTokens: 3, schemas.AttrUsageCost: 0.5,
		},
	}}}
	r := &recorder{}
	if err := New(Config{Subject: "evt.x"}, r, nil).Inject(context.Background(), trace); err != nil {
		t.Fatal(err)
	}
	ev := r.events[0]
	if r.subject != "evt.x" || ev.Status != "error" || ev.Error != "boom" || ev.Provider != "ollama" ||
		ev.InputTokens != 7 || ev.OutputTokens != 3 || ev.CostUSD != 0.5 {
		t.Errorf("%s %+v", r.subject, ev)
	}
}

func TestInjectSkipsATraceWithNoLLMCall(t *testing.T) {
	r := &recorder{}
	p := New(Config{}, r, nil)
	for _, tr := range []*schemas.Trace{nil, {Spans: []*schemas.Span{{Kind: schemas.SpanKindPlugin}}}, {Spans: []*schemas.Span{{Kind: schemas.SpanKindLLMCall}}}} {
		if err := p.Inject(context.Background(), tr); err != nil {
			t.Fatal(err)
		}
	}
	if len(r.events) != 0 {
		t.Errorf("published %d events", len(r.events))
	}
}

func TestHeaderPatternFollowsThePrefix(t *testing.T) {
	p := New(Config{HeaderPrefix: "X-App-"}, &recorder{}, nil)
	if got := p.RequestHeaderPatterns(); len(got) != 1 || got[0] != "x-app-*" {
		t.Errorf("%v", got)
	}
}

// The server gives completed traces only to plugins it registers as LLM
// plugins; without the hooks Inject is never called.
var (
	_ schemas.LLMPlugin           = (*Plugin)(nil)
	_ schemas.ObservabilityPlugin = (*Plugin)(nil)
)
