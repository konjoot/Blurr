/*
Сервис фонового выполнение отложенных заданий.

Приложение состоит из следующих блоков:

	* App - основная структура приложения, ее метод Run() - запускает мир
	* config - модуль конфига приложения
	* client.Client - клиент межсервисного взаимодействия
	* pool - модуль реализующий пул рутин (воркеров) выполняющих задания (jobs)
	* jobs - модуль где лежат все доступные задания, которые могут выполнять воркеры
	* jobs.Performer - интерфейс задания, все задания должны ему соотвествовать
	* hooks - здесь лежат хуки, пока используются только для тестирования
	* middleware - engine-совместимые middleware функции для использования очереди skat в приложениях поставляющих задания
	* queue - менеджер очереди
	* redis - слушатель редиса
	* registry - регистр доступных заданий, чтобы задание было здесь доступно нужно добавить соотв. строчку в jobs/manifest.go
	* taks - контейнер задания, инкапсулирует работу с очередью и заданиями
	* worker - nuff said

После запуска приложение запускает пул воркеров и слушает redis и tiker на
наличие сообщений. Получив сообщение приложение инициирует контейнер заданий Task
и отправляет его в пул. Как только Task попадает в пул воркер, который его получил
вызывает его метод Perform в котором осуществляется обращение к очереди и если там
что-то есть Task блокирует соотв. запись очереди инициирует и выполняет соотв. задание (Job).
Если задание завершается с ошибкой - оно возвращается в очередь, в случае
успешного выполнения - удаляется из очереди. Если выполнение задания завершилось
с ошибкой более трех раз и задание все еще в очереди - оно удаляется и логируется.

Задачи:
	* фоновое выполнение отложенных заданий
	* управление очередью

Зависимости:
	* Redis
*/
package skat

import (
	"os"
	"os/signal"
	"time"

	"golang.org/x/net/context"

	"github.com/konjoot/blurr/config"
	"github.com/konjoot/blurr/hooks"
	"github.com/konjoot/blurr/pool"
	"github.com/konjoot/blurr/task"
)

// Конструктор приложения, принимает конфиг
// возвращает инициализированный инстанс приложения
func New(conf *config.Config) *App {
	if conf == nil {
		conf = config.New()
	}

	app := &App{
		Config: conf,
		done:   make(chan struct{}),
	}

	return app
}

// Основная структура приложения
type App struct {
	// конфиг
	Config *config.Config
	// семафор завершения приложения
	done chan struct{}
}

// Запуск приложения
func (app *App) Run() {

	// инициализируем пул воркеров и получаем канал для отправки заданий (jobs) в работу
	pool, err := pool.New(app.Config.WorkerPoolSize, app.Config.Greedy)
	if err != nil {
		// логируем
		return
	}
	defer pool.Wait()

	// подписываемся на системные сигналы
	interrupt := make(chan os.Signal)
	signal.Notify(interrupt, os.Interrupt)

	// инитим тикер
	ticker := time.NewTicker(app.Config.Duration)
	defer ticker.Stop()

	keepWorking := true
	// основной цикл приложения
	for keepWorking {
		select {
		case <-ticker.C: // process jobs from ticker
			send(app, pool.In)
		case <-app.done: // shutdown by code
			keepWorking = false
		case <-interrupt: // shutdown by a system signal
			keepWorking = false
		}
	}

	// завершаем работу
	pool.Cancel()
}

// Завершает работу приложения
func (app *App) Stop() {
	close(app.done)
}

func (app *App) context() context.Context {
	ctx := context.Background()

	return ctx
}

// Неблокирующая отправка задания в пул
func send(app *App, in chan task.Performer) {
	select {
	case in <- &task.Task{Ctx: app.context()}:
		hooks.OnProcess()
	default:
	}
}
