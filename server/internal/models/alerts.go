package models

// LevelSeverity returns the canonical numeric severity for a log level, used
// by alert min-level matching and the persistence store. UNKNOWN is lowest so
// unclassified lines never satisfy a min-level threshold; the named levels
// ascend from TRACE to PANIC. Unrecognized values map to 0 like UNKNOWN.
func LevelSeverity(level LogLevel) int {
	switch level {
	case LogLevelTrace:
		return 1
	case LogLevelDebug:
		return 2
	case LogLevelInfo:
		return 3
	case LogLevelWarn:
		return 4
	case LogLevelError:
		return 5
	case LogLevelFatal:
		return 6
	case LogLevelPanic:
		return 7
	default:
		return 0
	}
}

// Alert is one fired-alert history entry.
type Alert struct {
	ID            string          `json:"id"`
	RuleID        string          `json:"ruleId"`
	RuleName      string          `json:"ruleName"`
	Type          string          `json:"type"`
	Host          string          `json:"host"`
	ContainerID   string          `json:"containerId"`
	ContainerName string          `json:"containerName"`
	Reason        string          `json:"reason"`
	Sample        string          `json:"sample,omitempty"`
	Count         int             `json:"count"`
	Suppressed    int             `json:"suppressed"`
	FiredAt       string          `json:"firedAt"`
	Delivery      *DeliveryResult `json:"delivery,omitempty"`
	// Channels holds the per-channel outcomes of the immutable trigger-time
	// channel snapshot. It is absent for history entries written before
	// per-channel tracking and for alerts with no enabled channels
	// (history-only). Targets and secrets are never stored here.
	Channels []ChannelDelivery `json:"channels,omitempty"`
}

// DeliveryResult records the outcome of one webhook delivery attempt.
type DeliveryResult struct {
	Status     string `json:"status"`
	HTTPStatus int    `json:"httpStatus,omitempty"`
	Error      string `json:"error,omitempty"`
}

// ChannelDelivery records the outcome for one channel in an alert's
// trigger-time snapshot. Only the stable channel identity and a redacted
// result are persisted — never the target URL, token, or other sending
// parameters.
type ChannelDelivery struct {
	ChannelID   string `json:"channelId"`
	ChannelName string `json:"channelName,omitempty"`
	ChannelType string `json:"channelType"`
	Status      string `json:"status"` // "ok" | "failed"
	HTTPStatus  int    `json:"httpStatus,omitempty"`
	Error       string `json:"error,omitempty"`
}
