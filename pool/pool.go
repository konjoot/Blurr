// Менеджер пула воркеров
package pool

import (
	"errors"
	"github.com/konjoot/blurr/hooks"
	"github.com/konjoot/blurr/task"
	"github.com/konjoot/blurr/worker"
	"runtime"
	"sync"
)

var (
	ErrSizeTooSmall = errors.New("pool size should be more then 0")
	ErrSizeTooLarge = errors.New("pool size should be less then 1000")
)

// Конструктор пула воркеров
func New(size int, greedy bool) (*Pool, error) {
	// валидируем размер пула
	if size < 1 {
		return nil, ErrSizeTooSmall
	}
	if size > 1000 {
		return nil, ErrSizeTooLarge
	}

	// инициализируем семафор для корректного завершения работы
	done := make(chan struct{})
	// из этого канала воркеры вычитывают задания от эмиттера
	// этот канал буферизирован для того, чтобы при работе в жадном режиме
	// не блокировать воркер при этом сохраняя хотя бы одну таску в буфере,
	// если другие воркеры в этот момент заняты, а очередь еще велика
	listen := make(chan task.Performer, 1)
	// чтобы достойно умереть
	wg := new(sync.WaitGroup)

	// запускаем пул воркеров
	wg.Add(size)
	for i := 0; i < size; i++ {
		go worker.New(i, wg.Done, done, listen, greedy)()
	}

	runtime.Gosched()

	// возвращаем инстанс пула
	return &Pool{
		In:   listen,
		wait: wg.Wait,
		done: done,
	}, nil
}

// Пул воркеров.
type Pool struct {
	// канал по которому воркеры принимают задания в работу
	In chan task.Performer
	// ожидалка завершения работы воркеров.
	wait func()
	// семафор завершения работы, все воркеры прекращают свою работу
	// как только этот канал закрывается
	done chan struct{}
}

// Завершает работу воркеров пула.
func (p *Pool) Cancel() {
	close(p.done)
}

func (p *Pool) Wait() {
	p.wait()
	hooks.OnPoolExit()
}
