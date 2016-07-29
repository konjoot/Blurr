// Контейнер задания.
package task

import (
	"database/sql"
	"errors"
	"fmt"
	"regulus/mrs"
	"runtime"
	"skat/client"
	"skat/hooks"
	"skat/jobs/interfaces"
	"skat/queue"
	"skat/registry"
	"time"

	. "engine/helpers"
	std "logger/helpers"
	. "logger/interfaces"
	. "skat/helpers/context"
	. "skat/helpers/logger"
	_ "skat/jobs"

	"engine"
	"golang.org/x/net/context"
)

var ErrTaskDeletedByCounter = errors.New("deleting task from queue because of counter overflow")
var ErrUnsupportedTask = errors.New("there are no job for a task")

type Task struct {
	DBM    *mrs.DBM
	Client client.Clienter
	Logger LogManager
	Redis  engine.RedisManager
	Ctx    context.Context
}

// Выполняет очередное задание из очереди
func (t *Task) Perform() int {

	dbh := t.DBM.DBH(t.Logger)

	ctx, cancel := context.WithCancel(t.Ctx)
	defer cancel()

	ctx = Queue_To(ctx, queue.NewProcesser(dbh, t.Redis))

	return perform(ctx, t.Logger, queue.New(dbh), registry.Find)
}

// непосредственно код, выполняющий задание
func perform(ctx context.Context, log LogManager, Q queue.Queuer, newJob interfaces.Finder) (count int) {

	// запрашиваем задание из очереди
	data, err := Q.Next()

	// очередь пуста, курим бамбук
	if err != nil && err == sql.ErrNoRows {
		return
	}

	// если ошибка возникла здесь, то мы ничего не можем поделать
	// поэтому логируем ошибку и выходим
	if err != nil {
		log.Print(std.Error(err))
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
			log.Print(Panic(data, fmt.Errorf("skat: panic jobing : %v\n%s", e, buf)))
			hooks.OnRecover()
			pushOrPop(log, Q, data)
		}
	}()

	// инициализиуем контекст для задания
	ctx = Logger_To(ctx, log.NewLogger(data.ID, data.Type))

	start := time.Now()
	end := func(data *queue.Data, err error) {
		if err != nil {
			log.Print(Error(data, err))
		}

		log.Print(std.Task(start, data.Type, err))
	}

	// инициируем задание в соответствии с данными из очереди
	job := newJob(data)

	// если задания для таска нет
	if job == nil {
		pushOrPop(log, Q, data)
		end(data, ErrUnsupportedTask)
		return
	}

	// выполняем задание
	hooks.OnJobPerform()
	if err := job.Perform(ctx); err != nil {
		pushOrPop(log, Q, data)
		end(data, err)
		return
	}

	// все прошло успешно - убираем задание из очереди
	end(data, Q.Pop(data.ID))
	return
}

// Если счетчик задания > 3 - удаляем из очереди
// в противном случае отправляем в конец очереди
func pushOrPop(log Logger, Q queue.Queuer, data *queue.Data) {
	// если счетчик задания > 3
	if data.Count > 3 {
		// удаляем его из очереди
		if err := Q.Pop(data.ID); err != nil {
			// не получилось удалить - логируем
			log.Print(Error(data, err))
		} else {
			// получилось удалить - логируем
			log.Print(Info(data, ErrTaskDeletedByCounter))
		}
	} else {
		// перемещаем задание в хвост очереди
		if err := Q.Push(data.ID); err != nil {
			// не получилось переместить - логируем
			log.Print(Error(data, err))
		}
	}
}
