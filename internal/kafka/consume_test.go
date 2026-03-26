package kafka

import (
	"context"
	"strings"
	"testing"
)

func TestConsumeMessage_ReaderNotInitialized(t *testing.T) {
	k := &Kafka{}
	err := k.ConsumeMessage(context.Background(), func(context.Context, []byte) error { return nil })
	if err == nil {
		t.Fatal("expected error when reader is nil")
	}
	if !strings.Contains(err.Error(), "not initialized") {
		t.Fatalf("err = %v", err)
	}
}
