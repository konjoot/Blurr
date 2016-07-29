// Наполнитель очереди
package queue

import (
	"engine"
	"regulus/mrs"

	. "engine/helpers"

	"github.com/pborman/uuid"
	"golang.org/x/net/context"
)

// Добавление нового задания в очередь
const ENQUEUE = `INSERT INTO queue.queue (id, type, meta, error) VALUES ($1, $2, $3, $4)`

// Конструктор поставщика новых заданий
func NewProcesser(dbh *mrs.DBH, rm engine.RedisManager) Processer {
	return &p{dbh, rm}
}

type p struct {
	dbh *mrs.DBH
	rm  engine.RedisManager
}

func (p *p) Process(ctx context.Context, data *Data) error {
	var err error

	if _, err = p.dbh.QBegin(); err != nil {
		return err
	}

	var storedErr string

	if data.Err != nil {
		storedErr = data.Err.Error()
	}

	if data.ID == "" {
		data.ID = uuid.New()
	}

	_, err = p.dbh.Exec(ENQUEUE, data.ID, data.Type, data.Meta, storedErr)

	p.dbh.CommitOrRollback(err)

	rh, err := p.rm.Handler(Logger_From(ctx))
	if err != nil {
		return err
	}

	defer rh.Close()

	// в данном случае нас не особо интересует успешность нотифая в редис
	// т.к. работу в очередь мы уже добавили, а значит она будет обработана
	// но ошибку все равно отправляем, чтобы мы могли ее корректно залогировать
	if rh != nil {
		_, err = rh.Do("PUBLISH", "queue", "do")
	}

	return err
}
