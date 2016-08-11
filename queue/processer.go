// Наполнитель очереди
package queue

import (
	"golang.org/x/net/context"
)

// Конструктор поставщика новых заданий
func NewProcesser() Processer {
	return &p{}
}

type p struct {
}

func (p *p) Process(ctx context.Context, data *Data) error {
	var err error

	return err
}
