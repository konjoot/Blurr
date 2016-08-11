// Контейнер задания.
package task

import (
	"errors"
	"fmt"
	"runtime"
	"skat/hooks"
	"skat/jobs/interfaces"
	"skat/queue"
	"skat/registry"
	"time"

	_ "skat/jobs"

	"golang.org/x/net/context"
)

var ErrTaskDeletedByCounter = errors.New("deleting task from queue because of counter overflow")
var ErrUnsupportedTask = errors.New("there are no job for a task")

type Task struct {
	ctx context.Context
}

// Выполняет очередное задание из очереди
func (t *Task) Perform() int {
	return perform(ctx, queue.New(), registry.Find)
}

// непосредственно код, выполняющий задание
func perform(ctx context.Context, Q queue.Queuer, newJob interfaces.Finder) (count int) {

	// запрашиваем задание из очереди
	data, err := Q.Next()

	// очередь пуста, курим бамбук
	if err == queue.ErrEmpty {
		return
	}

	// если ошибка возникла здесь, то мы ничего не можем поделать
	// поэтому логируем ошибку и выходим
	if err != nil {
		return
	}

	defer func() {
		count = Q.Count()
	}()

	// инкрементим счетчик задания
	data.Count++

	// если случилось страшное, перемещаем задание в хвост очереди
	defer func() {
		if e := recover(); e != nil {
			const size = 64 << 10
			buf := make([]byte, size)
			buf = buf[:runtime.Stack(buf, false)]
			// log.Print(Panic(data, fmt.Errorf("skat: panic jobing : %v\n%s", e, buf)))
			hooks.OnRecover()
			pushOrPop(Q, data)
		}
	}()

	// подготавливаем контекст для задания

	start := time.Now()
	end := func(data *queue.Data, err error) {
		// логирование и метрики
	}

	// инициируем задание в соответствии с данными из очереди
	job := newJob(data)

	// если задания для таска нет
	if job == nil {
		pushOrPop(Q, data)
		end(data, ErrUnsupportedTask)
		return
	}

	// выполняем задание
	hooks.OnJobPerform()
	if err := job.Perform(ctx); err != nil {
		pushOrPop(Q, data)
		end(data, err)
		return
	}

	// все прошло успешно - убираем задание из очереди
	end(data, Q.Pop(data.ID))
	return
}

// Если счетчик задания > 3 - удаляем из очереди
// в противном случае отправляем в конец очереди
func pushOrPop(Q queue.Queuer, data *queue.Data) {
	// если счетчик задания > 3
	if data.Count > 3 {
		// удаляем его из очереди
		if err := Q.Pop(data.ID); err != nil {
			// не получилось удалить - логируем
		} else {
			// получилось удалить - логируем
			// ErrTaskDeletedByCounter
		}
	} else {
		// перемещаем задание в хвост очереди
		if err := Q.Push(data.ID); err != nil {
			// не получилось переместить - логируем
		}
	}
}
