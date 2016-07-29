// Базовое задание. Используется в тестах
package base

import (
	"skat/jobs/interfaces"
	"skat/queue"

	"golang.org/x/net/context"
)

func New(*queue.Data) interfaces.Performer {
	return &BaseJob{}
}

type BaseJob struct{}

func (b *BaseJob) Perform(ctx context.Context) error {
	return nil
}
