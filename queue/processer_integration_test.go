// +build integration

package queue

import (
	"engine"
	"errors"
	"sync"

	. "engine/helpers/test"

	"github.com/garyburd/redigo/redis"
	"golang.org/x/net/context"
)

func (expect *ProcesserSuite) TestProcesser_Process() {
	rm := engine.NewRedis(TestRedisUri())

	ps, err := rm.PubSubConn(nil)
	expect.NoError(err)

	expect.NoError(ps.Subscribe("queue"))

	out := make(chan interface{}, 100)

	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		defer wg.Done()

		out <- ps.Receive()
		out <- ps.Receive()
		out <- ps.Receive()
	}()

	dbh := expect.DBM.DBH(nil)

	queue := NewProcesser(dbh, rm)

	var count int

	expect.NoError(dbh.QueryRow(`SELECT COUNT(1) FROM queue.queue`).Scan(&count))
	expect.Equal(0, count)

	expect.NoError(queue.Process(context.TODO(), &Data{ID: "id", Type: "type", Meta: []byte("{}")}))

	expect.NoError(dbh.QueryRow(`SELECT COUNT(1) FROM queue.queue`).Scan(&count))
	expect.Equal(1, count)

	var id, t, meta, er string
	expect.NoError(dbh.QueryRow(`SELECT id, type, meta, error FROM queue.queue WHERE id = 'id'`).Scan(&id, &t, &meta, &er))
	expect.Equal("id", id)
	expect.Equal("type", t)
	expect.Equal("{}", meta)
	expect.Equal("", er)

	expect.NoError(queue.Process(context.TODO(), &Data{ID: "id2", Type: "type1", Meta: []byte(`{"id":1}`), Err: errors.New("bad")}))

	expect.NoError(dbh.QueryRow(`SELECT id, type, meta, error FROM queue.queue WHERE id = 'id2'`).Scan(&id, &t, &meta, &er))
	expect.Equal("id2", id)
	expect.Equal("type1", t)
	expect.Equal(`{"id": 1}`, meta)
	expect.Equal("bad", er)

	wg.Wait()
	expect.Equal(redis.Subscription{Kind: "subscribe", Channel: "queue", Count: 1}, <-out)
	expect.Equal(redis.Message{Channel: "queue", Data: []uint8{0x64, 0x6f}}, <-out)
	expect.Equal(redis.Message{Channel: "queue", Data: []uint8{0x64, 0x6f}}, <-out)

	que := New(dbh)

	data, err := que.Next()
	expect.NoError(err)
	expect.Equal("id", data.ID)
	expect.Equal("type", data.Type)
	expect.Equal([]byte(`{}`), data.Meta)
	expect.Nil(data.Err)

	data, err = que.Next()
	expect.NoError(err)
	expect.Equal("id2", data.ID)
	expect.Equal("type1", data.Type)
	expect.Equal([]byte(`{"id": 1}`), data.Meta)
	expect.Equal(errors.New("bad"), data.Err)
}
