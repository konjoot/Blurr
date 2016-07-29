package notification

import (
	"event"
	"golang.org/x/net/context"
)

type Notifier interface {
	Send(ctx context.Context, e *event.Event) error
}
