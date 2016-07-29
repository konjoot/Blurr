// +build integration

package queue

import (
	"sync"

	"github.com/pborman/uuid"
)

func (expect *QueueSuite) TestQueue_Next_And_Unlock_Scenario() {
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
		(now() + interval '1 minute', $1),
		(now() + interval '2 minute', $2),
		(now() + interval '3 minute', $3),
		(now() + interval '4 minute', $4),
		(now() + interval '5 minute', $5),
		(now() + interval '6 minute', $6),
		(now() + interval '7 minute', $7),
		(now() + interval '8 minute', $8),
		(now() + interval '9 minute', $9),
		(now() + interval '10 minute', $10)`,
		id1, id2, id3, id4, id5, id6, id7, id8, id9, id10)

	expect.Nil(err)

	// there are three connections
	dbh1 := expect.DBM.DBH(expect.Logger.NewLogger("", ""))
	dbh2 := expect.DBM.DBH(expect.Logger.NewLogger("", ""))
	dbh3 := expect.DBM.DBH(expect.Logger.NewLogger("", ""))

	// and there are three queue objects
	queue1 := New(dbh1)
	queue2 := New(dbh2)
	queue3 := New(dbh3)

	// first queue locked first task
	data1, err := queue1.Next()
	expect.Nil(err)
	expect.Equal(id1, data1.ID)

	// second queue locked second task
	data21, err := queue2.Next()
	expect.Nil(err)
	expect.Equal(id2, data21.ID)

	// and third task
	data22, err := queue2.Next()
	expect.Nil(err)
	expect.Equal(id3, data22.ID)

	// and third queue locked fourth task
	data3, err := queue3.Next()
	expect.Nil(err)
	expect.Equal(id4, data3.ID)

	ids := make([]string, 6)

	// when concurrently used
	wg := new(sync.WaitGroup)
	wg.Add(6)
	for i := range ids {
		go func(i int) {
			defer wg.Done()

			dbh := expect.DBM.DBH(expect.Logger.NewLogger("", ""))
			queue := New(dbh)

			data, err := queue.Next()

			// we do not expect any errors
			expect.Nil(err)

			ids[i] = data.ID

			// finally tasks became unlocked and deleted from queue
			expect.Nil(queue.Pop(data.ID))

		}(i)
	}
	wg.Wait()

	// we expect right jobs will be selected
	expect.Len(ids, 6)

	expectedIDs := []string{id5, id6, id7, id8, id9, id10}
	for _, id := range ids {
		expect.Contains(expectedIDs, id)
	}

	// when we unlock first task
	_, err = dbh.Exec(UNLOCK, data1.ID)
	expect.Nil(err)

	// then it becomes available by other queries
	data23, err := queue2.Next()
	expect.Nil(err)
	expect.Equal(id1, data23.ID)

	// for second task too
	_, err = dbh.Exec(UNLOCK, data21.ID)
	expect.Nil(err)

	data12, err := queue1.Next()
	expect.Nil(err)
	expect.Equal(id2, data12.ID)
}
