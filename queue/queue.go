// Менеджер очереди

package queue

import (
	"errors"
)

var (
	ErrEmpty = errors.New("queue is empty")
)

// Конструктор менеджера очереди
func New() Queuer {
	return &q{}
}

type q struct {
}

type Data struct {
	ID    string
	Type  string
	Meta  []byte
	Count int
	Err   error
}

// Возвращает следующий элемент очереди
func (q *q) Next() (data *Data, err error) {
	data = &Data{}

	return data, nil
}

// Снимает блокировку и перемещаем задание в хвост очереди
func (q *q) Push(id string) (err error) {
	return nil
}

// Удаляет задание из очереди
func (q *q) Pop(id string) (err error) {
	return nil
}

// кол-во оставшихся заданий очереди
func (q *q) Count() (count int) {
	return
}
