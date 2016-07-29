// +build integration

package queue

import (
	"github.com/pborman/uuid"
)

func (expect *QueueSuite) TestQueue_Push_Scenario() {
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
		(timezone('utc'::text, now()) - interval '1 minute', $1),
		(timezone('utc'::text, now()) - interval '2 minute', $2),
		(timezone('utc'::text, now()) - interval '3 minute', $3),
		(timezone('utc'::text, now()) - interval '4 minute', $4),
		(timezone('utc'::text, now()) - interval '5 minute', $5),
		(timezone('utc'::text, now()) - interval '6 minute', $6),
		(timezone('utc'::text, now()) - interval '7 minute', $7),
		(timezone('utc'::text, now()) - interval '8 minute', $8),
		(timezone('utc'::text, now()) - interval '9 minute', $9),
		(timezone('utc'::text, now()) - interval '10 minute', $10)`,
		id1, id2, id3, id4, id5, id6, id7, id8, id9, id10)
	expect.Nil(err)

	// check queue state
	var id string
	err = dbh.QueryRow(`
		SELECT id FROM queue
		ORDER BY updated_at
		LIMIT 1`).Scan(&id)

	expect.Nil(err)
	expect.Equal(id10, id)

	err = dbh.QueryRow(`
		SELECT id FROM queue
		ORDER BY updated_at
		OFFSET ((SELECT COUNT(1) FROM queue) - 1)
		LIMIT 1`).Scan(&id)

	expect.Nil(err)
	expect.Equal(id1, id)

	queue := New(dbh)

	err = queue.Push(id10)
	expect.Nil(err)

	err = dbh.QueryRow(`
		SELECT id FROM queue
		ORDER BY updated_at
		LIMIT 1`).Scan(&id)

	expect.Nil(err)
	expect.Equal(id9, id)

	err = dbh.QueryRow(`
		SELECT id FROM queue
		ORDER BY updated_at
		OFFSET ((SELECT COUNT(1) FROM queue) - 1)
		LIMIT 1`).Scan(&id)

	expect.Nil(err)
	expect.Equal(id10, id)
}
