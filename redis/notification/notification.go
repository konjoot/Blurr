package notification

import (
	"encoding/json"
	"event"

	sn "skat/notification"

	"golang.org/x/net/context"

	. "engine/helpers"
	"errors"
	. "skat/helpers/context"
)

var (
	ErrEmptyRedis  = errors.New("redis manager is must")
	ErrEmptyLogger = errors.New("logger is must")
)

func NewNotifier() sn.Notifier {
	return &notification{}
}

type notification struct{}

func (n *notification) Send(ctx context.Context, e *event.Event) error {
	rdm := Redis_From(ctx)
	if rdm == nil {
		return ErrEmptyRedis
	}
	log := Logger_From(ctx)
	if log == nil {
		return ErrEmptyLogger
	}

	data, err := json.Marshal(e)
	if err != nil {
		return err
	}

	conn, err := rdm.Handler(log)
	if err != nil {
		return err
	}

	_, err = conn.Do("PUBLISH", "streams", data)

	conn.Close()

	return err
}
