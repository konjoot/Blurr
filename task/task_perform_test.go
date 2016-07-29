package task

import (
	"database/sql"
	"errors"
	"logger/msg"
	"logger/msg/levels"
	"logger/msg/types"
	"logger/tags"
	"skat/hooks"
	"skat/jobs/interfaces"
	"skat/queue"
	"testing"

	. "logger/interfaces"

	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
)

func TestTask_perform(t *testing.T) {
	var recovered int

	hooks.OnRecover = func() {
		recovered++
	}

	var que *testQue
	var j0b *testJob
	newJob := func(*queue.Data) interfaces.Performer {
		return j0b
	}
	nilJob := func(*queue.Data) interfaces.Performer {
		return nil
	}
	expect := assert.New(t)

	var log *logger

	log = &logger{}
	// when queue is empty
	j0b = &testJob{}
	que = &testQue{NextErr: sql.ErrNoRows}
	ctx := context.TODO()
	expect.Equal(0, perform(ctx, log, que, newJob))
	expect.False(que.Pushed)
	expect.False(que.Poped)
	expect.False(j0b.Called)
	expect.Nil(j0b.Ctx)
	expect.Zero(recovered)
	expect.Zero(log.buff)

	// when error on queue.Next()
	log = &logger{}
	j0b = &testJob{}
	que = &testQue{NextErr: errors.New("")}
	expect.Equal(0, perform(ctx, log, que, newJob))
	expect.False(que.Pushed)
	expect.False(que.Poped)
	expect.False(j0b.Called)
	expect.Nil(j0b.Ctx)
	expect.Zero(recovered)
	if expect.Len(log.buff, 1) {
		expect.Equal(&msg.Msg{Level: levels.ERROR, Type: types.ERROR}, log.buff[0])
	}

	// when next element of queue is available
	log = &logger{}
	j0b = &testJob{}
	que = &testQue{Data: &queue.Data{ID: "ID"}}
	expect.Equal(1, perform(ctx, log, que, newJob))
	expect.False(que.Pushed)
	expect.True(que.Poped)
	expect.True(j0b.Called)
	expect.Equal(1, que.Data.Count)
	expect.NotNil(j0b.Ctx.Value("Logger"))
	expect.Zero(recovered)
	expect.Len(log.buff, 1)

	// when there are no job for a task
	que = &testQue{Data: &queue.Data{ID: "ID"}}

	log = &logger{}
	que = &testQue{Data: &queue.Data{ID: "ID", Type: "type"}}
	expect.Equal(1, perform(ctx, log, que, nilJob))
	expect.True(que.Pushed)
	expect.False(que.Poped)
	expect.Equal(1, que.Data.Count)
	expect.Zero(recovered)
	if expect.Len(log.buff, 2) {
		expect.Equal(&msg.Msg{Level: levels.ERROR, Type: types.ERROR, Msg: "Data{ID: ID, Type: type, Meta: } - there are no job for a task"}, log.buff[0])
		expect.Equal(tags.Tags{"task", "type", "exit", "1"}, log.buff[1].Tags)
	}

	// when job.Perform returns error
	log = &logger{}
	j0b = &testJob{Err: errors.New("")}
	que = &testQue{Data: &queue.Data{ID: "ID", Count: 1}}

	expect.Equal(1, perform(ctx, log, que, newJob))
	expect.True(que.Pushed)
	expect.False(que.Poped)
	expect.True(j0b.Called)
	expect.Equal(2, que.Data.Count)
	expect.NotNil(j0b.Ctx.Value("Logger"))
	expect.Zero(recovered)
	if expect.Len(log.buff, 2) {
		expect.Equal(&msg.Msg{Level: levels.ERROR, Type: types.ERROR, Msg: "Data{ID: ID, Type: , Meta: } - "}, log.buff[0])
		expect.Equal(tags.Tags{"task", "", "exit", "1"}, log.buff[1].Tags)
	}

	// when data.Count > 3 and job.Perform returns error
	log = &logger{}
	j0b = &testJob{Err: errors.New("")}
	que = &testQue{Data: &queue.Data{ID: "ID", Count: 3}}
	expect.Equal(1, perform(ctx, log, que, newJob))
	expect.False(que.Pushed)
	expect.True(que.Poped)
	expect.True(j0b.Called)
	expect.Equal(4, que.Data.Count)
	expect.NotNil(j0b.Ctx.Value("Logger"))
	expect.Zero(recovered)
	if expect.Len(log.buff, 3) {
		expect.Equal(&msg.Msg{Level: levels.INFO, Msg: "Data{ID: ID, Type: , Meta: } - deleting task from queue because of counter overflow"}, log.buff[0])
		expect.Equal(&msg.Msg{Level: levels.ERROR, Type: types.ERROR, Msg: "Data{ID: ID, Type: , Meta: } - "}, log.buff[1])
		expect.Equal(tags.Tags{"task", "", "exit", "1"}, log.buff[2].Tags)
	}

	// when job.Perform fails with panic
	log = &logger{}
	j0b = &testJob{Panic: true}
	que = &testQue{Data: &queue.Data{ID: "ID", Count: 2}}
	expect.Equal(1, perform(ctx, log, que, newJob))
	expect.True(que.Pushed)
	expect.False(que.Poped)
	expect.True(j0b.Called)
	expect.Equal(3, que.Data.Count)
	expect.NotNil(j0b.Ctx.Value("Logger"))
	expect.Equal(1, recovered)
	if expect.Len(log.buff, 1) {
		expect.Equal(levels.ERROR, log.buff[0].Level)
		expect.Equal(types.ERROR, log.buff[0].Type)
	}

}

type testJob struct {
	Ctx    context.Context
	Called bool
	Panic  bool
	Err    error
}

func (j *testJob) Perform(ctx context.Context) error {
	j.Ctx = ctx
	j.Called = true

	if j.Panic {
		panic("job")
	}

	return j.Err
}

type testQue struct {
	Data    *queue.Data
	NextErr error
	Pushed  bool
	PushErr error
	Poped   bool
	PopErr  error
}

func (q *testQue) Next() (*queue.Data, error) {
	return q.Data, q.NextErr
}

func (q *testQue) Push(string) error {
	q.Pushed = true
	return q.PushErr
}

func (q *testQue) Pop(string) error {
	q.Poped = true
	return q.PopErr
}

func (q *testQue) Count() int {
	return 1
}

type logger struct {
	buff []*msg.Msg
}

func (l *logger) Print(msg *msg.Msg) {
	l.buff = append(l.buff, msg)
}

func (l *logger) Write(p []byte) (int, error) {
	l.buff = append(l.buff, msg.NewMsgFromByte(p))
	return len(p), nil
}

func (l *logger) NewLogger(string, string) LoggerCloser {
	return l
}

func (l *logger) AddTags(...string) {}
func (l *logger) Close()            {}
