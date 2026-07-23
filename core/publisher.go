package core

import (
	"github.com/praetordev/events"
)

// EventPublisher defines the interface for publishing events to the bus/stream.
type EventPublisher interface {
	PublishExecutionRequest(req *events.ExecutionRequest) error
	PublishJobEvent(event *events.JobEvent) error
}

// NOOPPublisher is a placeholder for development/testing
type NOOPPublisher struct{}

func (p *NOOPPublisher) PublishExecutionRequest(req *events.ExecutionRequest) error {
	// In a real implementation this would write to Kafka/NATS
	return nil
}

func (p *NOOPPublisher) PublishJobEvent(event *events.JobEvent) error {
	return nil
}
