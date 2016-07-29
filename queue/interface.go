package queue

import (
	"golang.org/x/net/context"
)

// Интерфейс менеджера очереди
type Queuer interface {
	Next() (*Data, error)
	Push(string) error
	Pop(string) error
	Count() int
}

// Интерфейс поставщика новых заданий очереди (исп. внешними приложениями)
type Processer interface {
	Process(context.Context, *Data) error
}
