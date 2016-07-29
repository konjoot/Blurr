// Слушатель редис
package redis

import (
	"engine"
	"errors"
	"skat/hooks"
	"sync"
	"time"

	. "logger/helpers"
	. "logger/interfaces"

	"github.com/garyburd/redigo/redis"
)

var ErrRedisManagerIsNil = errors.New("redis manager is must")
var ErrLoggerIsNil = errors.New("logger is must")
var ErrEmptyConn = errors.New("redis connection is not yet established")

const PS_CHAN = "queue"

// Конструктор слушателя редиса
func NewListener(rm engine.RedisManager, log Logger) (*listener, error) {
	// ошибка, если передан пустой пул
	if rm == nil {
		return nil, ErrRedisManagerIsNil
	}
	// или пустой логгер
	if log == nil {
		return nil, ErrLoggerIsNil
	}

	// в этот канал пишем нотофаи от редис
	out := make(chan struct{})
	// инициализируем семафор для корректного завершения работы
	done := make(chan struct{})
	// чтобы достойно умереть
	wg := new(sync.WaitGroup)

	listener := &listener{
		Out:  out,
		wait: wg.Wait,
		done: done,
	}

	subscriber := &subscribe{rm: rm, log: log, err: ErrEmptyConn}

	wg.Add(2)
	go read(wg.Done, done, log, subscriber.newConn, time.Minute, out)
	go monitor(wg.Done, done, subscriber.activeConn)

	return listener, nil
}

// Слушатель редис
type listener struct {
	// канал в который слушатель отправляет сообщения от редис
	Out chan struct{}
	// ожидалка завершения работы слушателя.
	wait func()
	// логгер
	done chan struct{}
}

// ждем завершения работы
func (l *listener) Wait() {
	l.wait()
	hooks.OnListenerExit()
}

// Завершает работу слушателя.
func (l *listener) Cancel() {
	close(l.done)
}

// слушает редис
func read(done func(), doneCh chan struct{}, log Logger, conn subscriber, dur time.Duration, out chan struct{}) {
	defer done()
	defer close(out)
	defer hooks.OnReadExit()

	hooks.OnReadStart()

	var ticker *time.Ticker

	ps, err := conn()
	if err != nil {
		log.Print(Error(err))
		goto reconnect
	}

listen:
	log.Print(Debug("start listening redis"))

	for {
		// здесь мы слушаем соотв redis канал
		switch msg := ps.Receive().(type) {
		case redis.Message, redis.Subscription:
			hooks.OnReadFanOut()
			out <- struct{}{}
		case error:

			log.Print(Error(msg))
			if msg == engine.ErrUnsubscribe || msg == engine.ErrPunsubscribe {
				ps.Close()
				return
			}

			log.Print(Debug("connection dropped"))
			goto reconnect
		}
	}

reconnect:
	log.Print(Debug("try to reconnect to redis"))
	ticker = time.NewTicker(dur)

	for {
		// здесь мы слушаем канал завершения и таймер,
		// по которому мы предпринимаем попытки реконнекта
		select {
		case <-doneCh:
			return
		case <-ticker.C:
			if ps, err = conn(); err == nil {
				hooks.OnReadReconnect()
				ticker.Stop()
				goto listen
			}
			log.Print(Error(err))
		}
	}
}

// обеспечивает корректное завершение работы функции read
func monitor(done func(), doneCh chan struct{}, conn subscriber) {
	defer done()

	defer func() {
		// если сейчас read слушает редис - отписываемся
		if ps, err := conn(); err == nil {
			ps.Unsubscribe()
		}
	}()

	defer hooks.OnMonitorExit()

	hooks.OnMonitorStart()

	<-doneCh
}

type pubsuber interface {
	Receive() interface{}
	Subscribe(...interface{}) error
	Unsubscribe(...interface{}) error
	Close() error
}

type subscriber func() (pubsuber, error)

type subscribe struct {
	rm     engine.RedisManager
	log    Logger
	active pubsuber
	err    error
}

func (s *subscribe) newConn() (ps pubsuber, err error) {
	defer func() {
		s.active = ps
		s.err = err
	}()

	ps, err = newPubSub(s.rm, s.log)
	if err != nil {
		return
	}

	err = ps.Subscribe(PS_CHAN)

	return
}

func (s *subscribe) activeConn() (pubsuber, error) {
	return s.active, s.err
}

// возвращает новое PubSub-подключение к редис
func newPubSub(rm engine.RedisManager, log Logger) (pubsuber, error) {
	return rm.PubSubConn(log)
}
