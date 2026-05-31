package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"

	"github.com/kirtipurohit/sagaforge-ai/internal/kafka"
	"github.com/kirtipurohit/sagaforge-ai/internal/models"
)

// triggeredEvents are the event types that warrant an AI analysis call.
var triggeredEvents = map[models.EventType]bool{
	models.EventOrderCreated:     true,
	models.EventPaymentFailed:    true,
	models.EventPaymentProcessed: true,
	models.EventOrderFailed:      true,
	models.EventOrderCompleted:   true,
}

// AzureOpenAIConfig holds Azure OpenAI connection details.
type AzureOpenAIConfig struct {
	Endpoint       string // e.g. https://your-resource.openai.azure.com
	APIKey         string
	DeploymentName string // e.g. gpt-4o
	APIVersion     string // e.g. 2024-08-01-preview
}

type InsightsService struct {
	pool   *pgxpool.Pool
	aiCfg  AzureOpenAIConfig
	client *http.Client
	log    *zap.Logger
}

func NewInsightsService(pool *pgxpool.Pool, aiCfg AzureOpenAIConfig, log *zap.Logger) *InsightsService {
	return &InsightsService{
		pool:   pool,
		aiCfg:  aiCfg,
		client: &http.Client{Timeout: 30 * time.Second},
		log:    log,
	}
}

func (s *InsightsService) Handle(ctx context.Context, msg kafkago.Message) error {
	event, err := kafka.Unmarshal[models.Event](msg)
	if err != nil {
		return err
	}
	if !triggeredEvents[event.Type] {
		return nil
	}

	// Fetch the saga_id if not already in the event
	sagaID := event.SagaID
	if sagaID == uuid.Nil {
		_ = s.pool.QueryRow(ctx, `SELECT id FROM sagas WHERE order_id = $1`, event.OrderID).Scan(&sagaID)
	}
	if sagaID == uuid.Nil {
		s.log.Debug("no saga found for event, skipping AI", zap.String("order_id", event.OrderID.String()))
		return nil
	}

	insight, err := s.generateInsight(ctx, event)
	if err != nil {
		// Non-fatal: log and continue rather than blocking the pipeline
		s.log.Error("AI insight generation failed", zap.Error(err), zap.String("order_id", event.OrderID.String()))
		return nil
	}
	insight.SagaID = sagaID

	return s.persistInsight(ctx, insight, event)
}

// ──────────────── Azure OpenAI Call ────────────────

// azureRequest / azureResponse model the Azure OpenAI chat completions API.
type azureRequest struct {
	Messages            []azureMessage `json:"messages"`
	MaxCompletionTokens int            `json:"max_completion_tokens"`
	Temperature         float64        `json:"temperature"`
}

type azureMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type azureResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

func (s *InsightsService) generateInsight(ctx context.Context, event models.Event) (*models.AIInsight, error) {
	payloadJSON, _ := json.MarshalIndent(event.Payload, "", "  ")

	prompt := fmt.Sprintf(`You are an AI analyst for an e-commerce order processing system.

An event just occurred in our distributed saga pipeline:

Event Type: %s
Order ID: %s
Saga ID: %s
Occurred At: %s
Payload:
%s

Please analyse this event and respond with a JSON object (and NOTHING else) in this exact format:
{
  "risk_score": <float 0.0 to 1.0, where 1.0 = highest risk>,
  "explanation": "<1–2 sentence explanation of the risk score>",
  "suggestion": "<1 concrete, actionable recommendation for the ops team>"
}

Guidelines:
- order.created: assess order size, customer pattern, potential fraud signals
- payment.failed: high risk — likely compensating transaction needed
- payment.processed: low risk if normal amount; flag if unusually large
- order.failed / fulfillment.failed: moderate-high risk — customer impact
- order.completed: low risk — confirm patterns look healthy`,
		event.Type, event.OrderID, event.SagaID, event.OccurredAt.Format(time.RFC3339), payloadJSON,
	)

	apiURL := fmt.Sprintf("%s/openai/deployments/%s/chat/completions?api-version=%s",
		strings.TrimRight(s.aiCfg.Endpoint, "/"),
		s.aiCfg.DeploymentName,
		s.aiCfg.APIVersion,
	)

	reqBody := azureRequest{
		Messages: []azureMessage{
			{Role: "system", Content: "You are a concise JSON-only risk analyst. Respond with valid JSON only, no markdown."},
			{Role: "user", Content: prompt},
		},
		MaxCompletionTokens: 512,
		Temperature:         0.3,
	}
	bodyBytes, _ := json.Marshal(reqBody)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, apiURL, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("api-key", s.aiCfg.APIKey)

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("azure openai call: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("azure openai %d: %s", resp.StatusCode, string(respBody))
	}

	var azResp azureResponse
	if err := json.Unmarshal(respBody, &azResp); err != nil {
		return nil, fmt.Errorf("decode azure response: %w", err)
	}
	if azResp.Error != nil {
		return nil, fmt.Errorf("azure openai error: %s", azResp.Error.Message)
	}
	if len(azResp.Choices) == 0 {
		return nil, fmt.Errorf("azure openai returned 0 choices")
	}

	responseText := strings.TrimSpace(azResp.Choices[0].Message.Content)

	// Strip markdown code fences if the model wraps the JSON
	if strings.HasPrefix(responseText, "```") {
		lines := strings.Split(responseText, "\n")
		responseText = strings.Join(lines[1:len(lines)-1], "\n")
	}

	var parsed struct {
		RiskScore   float64 `json:"risk_score"`
		Explanation string  `json:"explanation"`
		Suggestion  string  `json:"suggestion"`
	}
	if err := json.Unmarshal([]byte(responseText), &parsed); err != nil {
		return nil, fmt.Errorf("parse AI response: %w — raw: %s", err, responseText)
	}

	return &models.AIInsight{
		OrderID:      event.OrderID,
		TriggerEvent: event.Type,
		RiskScore:    parsed.RiskScore,
		Explanation:  parsed.Explanation,
		Suggestion:   parsed.Suggestion,
		GeneratedAt:  time.Now().UTC(),
	}, nil
}

// ──────────────── Persist ────────────────

func (s *InsightsService) persistInsight(ctx context.Context, insight *models.AIInsight, event models.Event) error {
	insightID := uuid.New()
	iKey := fmt.Sprintf("ai-insight-%s-%s", string(event.Type), event.OrderID)

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	if _, err := tx.Exec(ctx, `
		INSERT INTO ai_insights (id, saga_id, order_id, trigger_event, risk_score, explanation, suggestion, generated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT DO NOTHING
	`, insightID, insight.SagaID, insight.OrderID, string(insight.TriggerEvent),
		strconv.FormatFloat(insight.RiskScore, 'f', 3, 64),
		insight.Explanation, insight.Suggestion, insight.GeneratedAt); err != nil {
		return err
	}

	outboxEvent := models.Event{
		ID: uuid.New(), SagaID: insight.SagaID, OrderID: insight.OrderID,
		Type: models.EventAIInsightGenerated, OccurredAt: time.Now().UTC(),
		IdempotencyKey: iKey,
		Payload: map[string]any{
			"insight_id":    insightID,
			"trigger_event": insight.TriggerEvent,
			"risk_score":    insight.RiskScore,
			"explanation":   insight.Explanation,
			"suggestion":    insight.Suggestion,
		},
	}
	data, _ := json.Marshal(outboxEvent)
	if _, err := tx.Exec(ctx, `
		INSERT INTO outbox_events (aggregate_id, aggregate_type, event_type, payload, idempotency_key)
		VALUES ($1, 'ai_insight', $2, $3, $4)
		ON CONFLICT (idempotency_key) DO NOTHING
	`, insight.OrderID.String(), string(models.EventAIInsightGenerated), data, iKey); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}

	s.log.Info("AI insight stored",
		zap.String("order_id", insight.OrderID.String()),
		zap.Float64("risk_score", insight.RiskScore),
		zap.String("trigger", string(insight.TriggerEvent)),
	)
	return nil
}
