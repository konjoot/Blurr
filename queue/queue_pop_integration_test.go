// +build integration

package queue

import (
	"github.com/pborman/uuid"
)

func (expect *QueueSuite) TestQueue_Pop_Scenario() {
	dbh := expect.DBM.DBH(expect.Logger.NewLogger("", ""))

	id1 := uuid.New()
	id2 := uuid.New()
	id3 := uuid.New()
	id4 := uuid.New()
	id5 := uuid.New()
	id6 := uuid.New()
	id7 := uuid.New()
	id8 := uuid.New()
	id9 := uuid.New()
	id10 := uuid.New()

	_, err := dbh.Exec(`
		INSERT INTO queue (updated_at, id)
		VALUES
		(timezone('utc'::text, now()) + interval '1 year', $1),
		(timezone('utc'::text, now()) + interval '2 year', $2),
		(timezone('utc'::text, now()) + interval '3 year', $3),
		(timezone('utc'::text, now()) + interval '4 year', $4),
		(timezone('utc'::text, now()) + interval '5 year', $5),
		(timezone('utc'::text, now()) + interval '6 year', $6),
		(timezone('utc'::text, now()) + interval '7 year', $7),
		(timezone('utc'::text, now()) + interval '8 year', $8),
		(timezone('utc'::text, now()) + interval '9 year', $9),
		(timezone('utc'::text, now()) + interval '10 year', $10)`,
		id1, id2, id3, id4, id5, id6, id7, id8, id9, id10)
	expect.Nil(err)

	queue := New(dbh)

	// check queue state
	var id string

	err = dbh.QueryRow(`
		SELECT id FROM queue
		ORDER BY updated_at
		LIMIT 1`).Scan(&id)

	expect.Nil(err)
	expect.Equal(id1, id)

	expect.Equal(10, queue.Count())

	err = queue.Pop(id)
	expect.Nil(err)

	err = dbh.QueryRow(`
		SELECT id FROM queue
		ORDER BY updated_at
		LIMIT 1`).Scan(&id)

	expect.Nil(err)
	expect.Equal(id2, id)

	expect.Equal(9, queue.Count())
}
