// Менеджер очереди

package queue

import (
	"errors"
	"regulus/mrs"
)

// Следующее задание очереди
const NEXT = `
		SELECT
			id,
			type,
			meta,
			count,
			error
		FROM queue
		WHERE lock = false
		ORDER BY updated_at
		FOR UPDATE OF queue SKIP LOCKED
		LIMIT 1`

// Перемещение задания в хвост очереди
const PUSH = `
		UPDATE queue
		SET
			updated_at = timezone('utc'::text, now()),
			count = count+1
		WHERE id = $1`

// Удаляем задание из очереди
const POP = `DELETE FROM queue WHERE id = $1`

// Блокировка
const UNLOCK = `UPDATE queue SET lock = false where id = $1`

// Снятие блокировки
const LOCK = `UPDATE queue SET lock = true where id = $1`

// Получение элемента очереди для обновления
const SELECT = `
		SELECT
			id
		FROM queue
		WHERE id = $1
		FOR UPDATE OF queue SKIP LOCKED
		LIMIT 1`

// Кол-во задач в очереди
const COUNT = `
	SELECT
		count(1)
	FROM
		queue
	WHERE
		lock = false`

// Конструктор менеджера очереди
func New(dbh *mrs.DBH) Queuer {
	return &q{dbh: dbh}
}

type q struct {
	dbh *mrs.DBH
}

type Data struct {
	ID    string
	Type  string
	Meta  []byte
	Count int
	Err   error
}

// Возвращает следующий элемент очереди
func (q *q) Next() (data *Data, err error) {
	data = &Data{}

	if _, err = q.dbh.QBegin(); err != nil {
		return
	}

	defer func() {
		q.dbh.CommitOrRollback(err)
	}()

	var storedErr string
	// запрашиваем следующую незаблокированную запись из очереди и пытаемся повесить на нее блокировку
	if err = q.dbh.QueryRow(NEXT).Scan(&data.ID, &data.Type, &data.Meta, &data.Count, &storedErr); err != nil {
		return
	}

	if storedErr != "" {
		data.Err = errors.New(storedErr)
	}

	if _, err = q.dbh.Exec(LOCK, data.ID); err != nil {
		return
	}

	return
}

// Снимает блокировку и перемещаем задание в хвост очереди
func (q *q) Push(id string) (err error) {

	if _, err = q.dbh.QBegin(); err != nil {
		return
	}

	defer func() {
		q.dbh.CommitOrRollback(err)
	}()

	if _, err = q.dbh.Exec(SELECT, id); err != nil {
		return
	}

	if _, err = q.dbh.Exec(PUSH, id); err != nil {
		return
	}

	_, err = q.dbh.Exec(UNLOCK, id)

	return
}

// Удаляет задание из очереди
func (q *q) Pop(id string) (err error) {
	if _, err = q.dbh.QBegin(); err != nil {
		return
	}

	defer func() {
		q.dbh.CommitOrRollback(err)
	}()

	if _, err = q.dbh.Exec(SELECT, id); err != nil {
		return
	}

	_, err = q.dbh.Exec(POP, id)

	return
}

// кол-во оставшихся заданий очереди
func (q *q) Count() (count int) {

	q.dbh.QueryRow(COUNT).Scan(&count)

	return
}
