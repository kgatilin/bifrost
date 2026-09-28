// Package natslog is a fork-local observability plugin: every completed LLM
// request is published as one JSON event to a NATS JetStream subject, with its
// provider, model, tokens, Bifrost's computed cost, latency and the caller's
// x-bf-lh-* headers.
//
// It lives inside the transports module, not under plugins/, so the fork adds
// no Go module of its own and the upstream Dockerfile builds it unchanged.
package natslog

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/maximhq/bifrost/core/schemas"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// PluginName is the name under `plugins` in config.json.
const PluginName = "natslog"

// Config is the plugin's `config` in config.json.
type Config struct {
	// URL of the NATS server; empty reads NATS_URL from the environment.
	URL string `json:"url,omitempty"`
	// Subject events are published on; default evt.llm.call.
	Subject string `json:"subject,omitempty"`
	// HeaderPrefix selects the request headers carried into the event, with the
	// prefix stripped from their names; default x-bf-lh-.
	HeaderPrefix string `json:"header_prefix,omitempty"`
}

// Event is one LLM request as published.
type Event struct {
	At           time.Time         `json:"at"`
	RequestID    string            `json:"request_id,omitempty"`
	Provider     string            `json:"provider"`
	Model        string            `json:"model"`
	RequestType  string            `json:"request_type,omitempty"`
	Status       string            `json:"status"`
	Error        string            `json:"error,omitempty"`
	InputTokens  int               `json:"input_tokens,omitempty"`
	OutputTokens int               `json:"output_tokens,omitempty"`
	TotalTokens  int               `json:"total_tokens,omitempty"`
	CostUSD      float64           `json:"cost_usd,omitempty"`
	LatencyMs    int64             `json:"latency_ms"`
	Headers      map[string]string `json:"headers,omitempty"`
}

// Publisher is the part of JetStream the plugin uses.
type Publisher interface {
	Publish(ctx context.Context, subject string, data []byte) error
}

type Plugin struct {
	cfg    Config
	pub    Publisher
	conn   *nats.Conn
	logger schemas.Logger
}

// Init connects to NATS. The connection reconnects on its own; a request whose
// publish fails is logged and dropped, never failed.
func Init(cfg *Config, logger schemas.Logger) (*Plugin, error) {
	c := Config{}
	if cfg != nil {
		c = *cfg
	}
	if c.URL == "" {
		c.URL = os.Getenv("NATS_URL")
	}
	if c.URL == "" {
		return nil, fmt.Errorf("%s: no url in config and NATS_URL is not set", PluginName)
	}
	nc, err := nats.Connect(c.URL, nats.Name("bifrost-"+PluginName), nats.MaxReconnects(-1))
	if err != nil {
		return nil, fmt.Errorf("%s: connect: %w", PluginName, err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("%s: jetstream: %w", PluginName, err)
	}
	p := New(c, jsPublisher{js}, logger)
	p.conn = nc
	return p, nil
}

// New builds the plugin over any publisher; Init is the NATS one.
func New(cfg Config, pub Publisher, logger schemas.Logger) *Plugin {
	if cfg.Subject == "" {
		cfg.Subject = "evt.llm.call"
	}
	if cfg.HeaderPrefix == "" {
		cfg.HeaderPrefix = "x-bf-lh-"
	}
	cfg.HeaderPrefix = strings.ToLower(cfg.HeaderPrefix)
	return &Plugin{cfg: cfg, pub: pub, logger: logger}
}

type jsPublisher struct{ js jetstream.JetStream }

func (j jsPublisher) Publish(ctx context.Context, subject string, data []byte) error {
	_, err := j.js.Publish(ctx, subject, data)
	return err
}

func (p *Plugin) GetName() string { return PluginName }

func (p *Plugin) Cleanup() error {
	if p.conn != nil {
		return p.conn.Drain()
	}
	return nil
}

// The plugin reads neither message content nor plugin spans, so the tracer
// need not build them for it.
func (p *Plugin) ConsumesContent() bool     { return false }
func (p *Plugin) ConsumesPluginSpans() bool { return false }

// RequestHeaderPatterns asks the tracer to capture the caller's headers.
func (p *Plugin) RequestHeaderPatterns() []string { return []string{p.cfg.HeaderPrefix + "*"} }

// Inject publishes the request's last LLM attempt. A trace with none (a
// request rejected before dispatch) publishes nothing.
func (p *Plugin) Inject(ctx context.Context, trace *schemas.Trace) error {
	ev, ok := p.event(trace)
	if !ok {
		return nil
	}
	data, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	if err := p.pub.Publish(ctx, p.cfg.Subject, data); err != nil {
		if p.logger != nil {
			p.logger.Warn("%s: publish %s: %v", PluginName, p.cfg.Subject, err)
		}
		return err
	}
	return nil
}

func (p *Plugin) event(trace *schemas.Trace) (Event, bool) {
	if trace == nil {
		return Event{}, false
	}
	var last *schemas.Span
	for _, s := range trace.Spans {
		if s == nil || (s.Kind != schemas.SpanKindLLMCall && s.Kind != schemas.SpanKindRetry) {
			continue
		}
		if last == nil || s.EndTime.After(last.EndTime) {
			last = s
		}
	}
	if last == nil {
		return Event{}, false
	}
	ev := Event{
		At:        last.EndTime,
		RequestID: trace.RequestID,
		Status:    "ok",
		LatencyMs: trace.EndTime.Sub(trace.StartTime).Milliseconds(),
	}
	if last.Status == schemas.SpanStatusError {
		ev.Status, ev.Error = "error", last.StatusMsg
	}
	a := last.Attributes
	ev.Provider = str(a[schemas.AttrProviderName])
	ev.Model = str(a[schemas.AttrRequestModel])
	ev.RequestType = str(a[schemas.AttrOperationName])
	ev.InputTokens = num[int](a[schemas.AttrInputTokens])
	ev.OutputTokens = num[int](a[schemas.AttrOutputTokens])
	ev.TotalTokens = num[int](a[schemas.AttrTotalTokens])
	ev.CostUSD = num[float64](a[schemas.AttrUsageCost])
	if ev.Provider == "" && strings.TrimSpace(ev.Model) == "" {
		return Event{}, false
	}
	for k, v := range trace.RequestHeaders {
		if name, ok := strings.CutPrefix(k, p.cfg.HeaderPrefix); ok && name != "" {
			if ev.Headers == nil {
				ev.Headers = map[string]string{}
			}
			ev.Headers[name] = v
		}
	}
	return ev, true
}

func str(v any) string {
	s, _ := v.(string)
	return s
}

// num reads an attribute the tracer may have stored as any numeric type.
func num[T int | float64](v any) T {
	switch n := v.(type) {
	case int:
		return T(n)
	case int32:
		return T(n)
	case int64:
		return T(n)
	case float32:
		return T(n)
	case float64:
		return T(n)
	}
	return 0
}
