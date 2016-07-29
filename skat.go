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
	"database/sql"
	"engine"
	"engine/deps"
	"logger"
	"os"
	"os/signal"
	"regulus/mrs"
	"skat/client"
	"skat/config"
	"skat/hooks"
	"skat/pool"
	"skat/redis"
	"skat/task"
	"statsd"
	"time"

	as "logger/helpers"
	mizar "mizar/client"
	regulus "regulus/client"
	rn "skat/redis/notification"
	vega "vega/client"

	. "engine/deps/helpers"
	_ "github.com/lib/pq"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	. "logger/interfaces"
	"proto/alcor/groups"
	"proto/alcor/users"
	"proto/sirius/messages"
	. "skat/helpers/context"

	"proto/sirius/attachments"
	"proto/sirius/rooms"
	"skat/notification"
)

// Конструктор приложения, принимает конфиг
// возвращает инициализированный инстанс приложения
func New(conf *config.Config) *App {
	if conf == nil {
		conf = config.New()
	}

	logger := logger.NewManager()
	logger.AddTags(`service`, `skat`)

	db, err := sql.Open("postgres", conf.DB)
	if err != nil {
		logger.Print(as.Error(err))
		return nil
	}

	rm := engine.NewRedis(conf.RedisURL)

	// клиент к мизару
	mizar, err := mizar.New(conf.MizarURL)
	if err != nil {
		logger.Print(as.Error(err))
		return nil
	}

	// клиент к веге
	vega, err := vega.New(conf.VegaURL)
	if err != nil {
		logger.Print(as.Error(err))
		return nil
	}

	// клиент к регулусу
	regulus := regulus.New(conf.RegulusURL)

	// клиент межсерверного взаимодействия
	client := client.New(vega, mizar, regulus, conf.Login, conf.Pass)

	// Конфигурация пула коннектов к БД
	db.SetMaxOpenConns(conf.DBPoolSize * 3 / 2)
	db.SetMaxIdleConns(conf.DBPoolSize)

	dbm := mrs.NewDBM(db)

	// регистрируем зависимости для дальнейшего мониторинга доступности
	deps.Add(Redis(rm.Check))
	deps.Add(DB(dbm.Check))

	app := &App{
		DBM:      dbm,
		RM:       rm,
		Logger:   logger,
		Client:   client,
		Notifier: rn.NewNotifier(),

		Config: conf,
		done:   make(chan struct{}),
	}

	if conf.GrpcOn {

		var opts []grpc.DialOption

		if conf.TLSOn {
			var sn string
			if conf.TLSHostOverride != "" {
				sn = conf.TLSHostOverride
			}
			var creds credentials.TransportCredentials
			if conf.CA != "" {
				var err error
				creds, err = credentials.NewClientTLSFromFile(conf.CA, sn)
				if err != nil {
					app.Logger.Print(as.Error(err))
				}
			} else {
				creds = credentials.NewClientTLSFromCert(nil, sn)
			}
			opts = append(opts, grpc.WithTransportCredentials(creds))
		} else {
			opts = append(opts, grpc.WithInsecure())
		}

		siriusConn, err := grpcConn(conf.SiriusAddr, opts...)
		if err != nil {
			logger.Print(as.Error(err))
			return nil
		}

		siriusMessages := messages.NewMessagesClient(siriusConn)
		siriusAttachments := attachments.NewAttachmentsClient(siriusConn)
		siriusRooms := rooms.NewRoomClient(siriusConn)

		alcorConn, err := grpcConn(conf.AlcorAddr, opts...)
		if err != nil {
			logger.Print(as.Error(err))
			return nil
		}

		alcorGroups := groups.NewGroupsClient(alcorConn)
		alcorUsers := users.NewUsersClient(alcorConn)

		app.AlcorGroups = alcorGroups
		app.AlcorUsers = alcorUsers
		app.SiriusMessages = siriusMessages
		app.SiriusAttachments = siriusAttachments
		app.SiriusRooms = siriusRooms
	}

	return app
}

// Основная структура приложения
type App struct {
	// пулл коннектов к БД
	DBM *mrs.DBM
	// пулл коннектов к редис
	RM engine.RedisManager
	// конфиг
	Config *config.Config
	// логгер
	Logger LogManager
	// клиент межсервисного взаимодействия
	Client client.Clienter

	// grpc clients
	AlcorGroups       groups.GroupsClient
	AlcorUsers        users.UsersClient
	SiriusMessages    messages.MessagesClient
	SiriusAttachments attachments.AttachmentsClient
	SiriusRooms       rooms.RoomClient

	Notifier notification.Notifier

	// семафор завершения приложения
	done chan struct{}
}

func grpcConn(addr string, opts ...grpc.DialOption) (conn *grpc.ClientConn, err error) {
	conn, err = grpc.Dial(addr, opts...)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// Запуск приложения
func (app *App) Run() {
	statsd.Init()

	// инициализируем пул воркеров и получаем канал для отправки заданий (jobs) в работу
	pool, err := pool.New(app.Config.WorkerPoolSize, app.Config.Greedy)
	if err != nil {
		app.Logger.Print(as.Error(err))
		return
	}
	defer pool.Wait()

	// инициализиуем слушатель
	listener, err := redis.NewListener(app.RM, app.Logger)
	if err != nil {
		app.Logger.Print(as.Error(err))
		return
	}
	defer listener.Wait()

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
		case <-listener.Out: // process jobs from sender
			send(app, pool.In)
		case <-ticker.C: // process jobs from ticker
			send(app, pool.In)
		case <-app.done: // shutdown by code
			keepWorking = false
		case <-interrupt: // shutdown by a system signal
			keepWorking = false
		}
	}

	// завершаем работу
	listener.Cancel()
	pool.Cancel()
}

// Завершает работу приложения
func (app *App) Stop() {
	close(app.done)
}

func (app *App) context() context.Context {
	ctx := context.Background()

	ctx = Client_To(ctx, app.Client)
	ctx = Redis_To(ctx, app.RM)
	ctx = AlcorGroups_To(ctx, app.AlcorGroups)
	ctx = AlcorUsers_To(ctx, app.AlcorUsers)
	ctx = SiriusMessages_To(ctx, app.SiriusMessages)
	ctx = SiriusAttachments_To(ctx, app.SiriusAttachments)
	ctx = SiriusRooms_To(ctx, app.SiriusRooms)
	ctx = Notifier_To(ctx, app.Notifier)

	return ctx
}

// Неблокирующая отправка задания в пул
func send(app *App, in chan task.Performer) {
	select {
	case in <- &task.Task{DBM: app.DBM, Logger: app.Logger, Client: app.Client, Redis: app.RM, Ctx: app.context()}:
		hooks.OnProcess()
	default:
	}
}
