package metrics

import "go.opentelemetry.io/otel/attribute"

func statusAttr(status string) attribute.KeyValue    { return attribute.String("status", status) }
func transportAttr(t Transport) attribute.KeyValue   { return attribute.String("transport", string(t)) }
func reasonAttr(r ConflictReason) attribute.KeyValue { return attribute.String("reason", string(r)) }
func kindAttr(k RetryKind) attribute.KeyValue        { return attribute.String("kind", string(k)) }
