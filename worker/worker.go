// Воркер пула
package worker

import (
	"github.com/konjoot/blurr/hooks"
	"github.com/konjoot/blurr/task"
	"runtime"
)

// Kонструктор воркера
func New(i int, done func(), doneCh chan struct{}, listen chan task.Performer, greedy bool) func() {
	worker := func() {
		defer done()

		defer hooks.OnWorkerExit(i)

		hooks.OnWorkerStart(i)

		for {
			select {
			case <-doneCh:
				return
			case task := <-listen:

				// если в очереди еще остались таски и пул работает в жадном режиме
				if task.Perform() > 0 && greedy {
					// переотправляем таск в очередь
					select {
					case listen <- task:
					default:
					}

				}

				hooks.OnTaskFinish(i)

				// чтобы нагрузка равномерно распределялась между
				// воркерами, сразу после выполнения таска
				// воркер уступает системный тред
				runtime.Gosched()
			}
		}
	}

	return worker
}
