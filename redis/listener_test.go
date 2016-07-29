package redis

import (
	"engine"
	"errors"
	"fmt"
	"logger"
	"runtime"
	"skat/hooks"
	"testing"
	"time"

	. "engine/helpers/test"

	"github.com/garyburd/redigo/redis"
	"github.com/stretchr/testify/assert"
)

func TestListener(t *testing.T) {
	expect := assert.New(t)

	read := make(chan struct{})
	monitor := make(chan struct{})
	finish := make(chan string)
	done := make(chan struct{})
	checkDone := make(chan struct{})

	hooks.OnReadStart = func() {
		read <- struct{}{}
	}
	hooks.OnMonitorStart = func() {
		monitor <- struct{}{}
	}
	hooks.OnReadExit = func() {
		finish <- "read"
	}
	hooks.OnMonitorExit = func() {
		finish <- "monitor"
	}

	rm := engine.NewRedis(TestRedisUri())

	listener, err := NewListener(rm, logger.Silent())
	expect.Nil(err)

	// waiting for read and monitor rutines
	<-read
	<-monitor

	rh, err := rm.Handler(nil)
	expect.Nil(err)

	go func() {
		for i := 0; i < 10; i++ {
			rh.Do("PUBLISH", "queue", fmt.Sprintf("%d", i))
			runtime.Gosched()
		}

		close(done)
	}()

	go func() {
		order := make([]string, 0)

		for {
			select {
			case <-done:
			case <-listener.Out:
			case body := <-finish:
				order = append(order, body)
			case <-checkDone:
				goto check
			}
		}

	check:
		expect.Equal([]string{"monitor", "read"}, order)
		return
	}()

	<-done

	listener.Cancel()
	listener.Wait()
	close(checkDone)
}

func TestMonitor(t *testing.T) {

	hooks.Reset()

	expect := assert.New(t)
	finish := make(chan struct{}, 1)
	doneCh := make(chan struct{})

	done := func() { finish <- struct{}{} }

	// when there dropped connection
	sub := &testSubscribe{err: ErrEmptyConn}

	go monitor(done, doneCh, sub.activeConn)

	close(doneCh)

	expect.Equal(struct{}{}, <-finish)

	expect.True(sub.activeConnCalled)

	rm := engine.NewRedis(TestRedisUri())

	ps, err := newPubSub(rm, logger.Silent())
	expect.NoError(err)

	expect.NoError(ps.Subscribe("comechan"))
	expect.EqualValues(redis.Subscription{Kind: "subscribe", Channel: "comechan", Count: 1}, ps.Receive())

	// when there good connection
	sub = &testSubscribe{active: ps}
	doneCh = make(chan struct{})

	go monitor(done, doneCh, sub.activeConn)

	close(doneCh)

	expect.Equal(struct{}{}, <-finish)

	expect.True(sub.activeConnCalled)

	expect.EqualValues(engine.ErrUnsubscribe, ps.Receive())
}

func TestReadReconnect(t *testing.T) {
	hooks.Reset()

	expect := assert.New(t)

	finish := make(chan struct{})
	readFinish := make(chan struct{})
	doneCh := make(chan struct{})
	reconnect := make(chan struct{})
	out := make(chan struct{}, 10000)
	outCh := make(chan struct{})
	hooks.OnReadReconnect = func() { reconnect <- struct{}{} }
	hooks.OnReadFanOut = func() { outCh <- struct{}{} }

	done := func() { readFinish <- struct{}{} }

	badCalls := make(chan struct{})
	badClosed := make(chan struct{})
	goodCalls := make(chan struct{})
	goodClosed := make(chan struct{})
	var bcalls, bclosed, gcalls, gclosed, reconnected, outed int

	go func() {
		for {
			select {
			case <-doneCh:
				goto check
			case <-badCalls:
				bcalls++
			case <-badClosed:
				bclosed++
			case <-goodCalls:
				gcalls++
			case <-goodClosed:
				gclosed++
			case <-reconnect:
				reconnected++
			case <-outCh:
				outed++
			}
		}

	check:
		expect.Equal(1, bcalls)
		expect.Equal(0, bclosed)
		expect.Equal(2, gcalls)
		expect.Equal(1, gclosed)
		expect.Equal(1, reconnected)
		expect.Equal(1, outed)
		finish <- struct{}{}

		return
	}()

	badConn := &testSubscription{msg: errors.New("bad connections"), calls: badCalls, closed: badClosed}
	goodConn := &testSubscription{msg: redis.Message{}, calls: goodCalls, closed: goodClosed}

	sub := &testSubscribe{reCount: 2, active: badConn, conn: goodConn}

	go read(done, make(chan struct{}), logger.Silent(), sub.newConn, time.Microsecond, out)

	doneCh <- <-readFinish

	expect.Zero(sub.reCount)

	time.Sleep(time.Microsecond)

	<-finish
}

func TestListener_New(t *testing.T) {
	expect := assert.New(t)
	hooks.Reset()

	rm := engine.NewRedis(TestRedisUri())

	listener, err := NewListener(nil, logger.Silent())
	expect.Equal(ErrRedisManagerIsNil, err)
	expect.Nil(listener)
	listener, err = NewListener(rm, nil)
	expect.Equal(ErrLoggerIsNil, err)
	expect.Nil(listener)
}

type testSubscribe struct {
	active           pubsuber
	conn             pubsuber
	reCount          int
	err              error
	activeConnCalled bool
}

func (s *testSubscribe) activeConn() (pubsuber, error) {
	s.activeConnCalled = true
	return s.active, s.err
}

func (s *testSubscribe) newConn() (pubsuber, error) {
	if s.reCount > 0 {
		s.reCount--

		if s.active != nil {
			conn := s.active
			s.active = nil
			return conn, nil
		}
		return nil, errors.New("bad")
	}

	return s.conn, nil
}

type testSubscription struct {
	pubsuber
	count  int
	calls  chan struct{}
	closed chan struct{}
	msg    interface{}
}

func (t *testSubscription) Receive() interface{} {
	t.calls <- struct{}{}

	t.count++
	if t.count > 1 {
		return engine.ErrUnsubscribe
	}

	return t.msg
}

func (t *testSubscription) Close() error {
	t.closed <- struct{}{}
	return nil
}
