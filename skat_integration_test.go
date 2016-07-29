// +build integration

package skat

import (
	"database/sql"
	"engine"
	"fmt"
	"log"
	"logger/helpers/test"
	"os"
	"regulus/mrs"
	"runtime"
	"skat/config"
	"skat/hooks"
	"sync"
	"testing"
	"time"

	. "engine/helpers/test"
	_ "github.com/lib/pq"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
)

const (
	SCHEMA_NAME = "queue"
	SCHEMA_PATH = "./migrations/schema.sql"
	DB_NAME     = "skat_test_db_suite"
)

func TestSkat(t *testing.T) {
	Skat_Ticker_Scenario(t)
	Skat_Listener_Scenario(t)
	Skat_Greedy_Scenario(t)
}

func Skat_Ticker_Scenario(t *testing.T) {
	hooks.Reset()
	expect := assert.New(t)

	var listenerExited, poolExited bool
	triggered := make(chan struct{}, 1000)
	processed := make(chan struct{}, 1000)

	finished := make(chan int, 100)

	hooks.OnProcess = func() {
		triggered <- struct{}{}
	}
	hooks.OnJobPerform = func() {
		processed <- struct{}{}
	}
	hooks.OnListenerExit = func() {
		listenerExited = true
	}
	hooks.OnPoolExit = func() {
		poolExited = true
	}
	hooks.OnTaskFinish = func(i int) {
		finished <- i
	}

	// here we use incorrect redis url to check gracefull shutdown when listener is reconnecting
	app, clean := newSkat(time.Millisecond, t, "redis://localhost:6377", false)
	defer clean()

	dbh := app.DBM.DBH(app.Logger)

	for i := 0; i < 10; i++ {
		_, err := dbh.Exec(`INSERT INTO queue (id) VALUES ($1)`, uuid.New())
		expect.Nil(err)
	}
	var count int

	expect.Nil(dbh.QueryRow(`SELECT COUNT(1) FROM queue`).Scan(&count))
	expect.Equal(10, count)

	wg := new(sync.WaitGroup)
	wg.Add(1)

	go func() {
		defer wg.Done()
		app.Run()
	}()

	runtime.Gosched()

	// waiting for workers
	for i := 0; i < 10; i++ {
		<-finished
	}

	app.Stop()

	wg.Wait()

	expect.Nil(dbh.QueryRow(`SELECT COUNT(1) FROM queue`).Scan(&count))
	expect.Zero(count)

	expect.True(listenerExited)
	expect.True(poolExited)
	expect.Len(processed, 10)
	// expect.True(triggered > 10 && triggered < 100)
	expect.True(len(triggered) > 10)
}

func Skat_Listener_Scenario(t *testing.T) {
	hooks.Reset()
	expect := assert.New(t)

	var listenerExited, poolExited bool
	finished := make(chan int, 100)
	triggered := make(chan struct{}, 1000)
	processed := make(chan struct{}, 1000)
	done := make(chan struct{})

	hooks.OnProcess = func() {
		triggered <- struct{}{}
	}
	hooks.OnJobPerform = func() {
		processed <- struct{}{}
	}
	hooks.OnListenerExit = func() {
		listenerExited = true
	}
	hooks.OnPoolExit = func() {
		poolExited = true
	}
	hooks.OnTaskFinish = func(i int) {
		finished <- i
	}

	app, clean := newSkat(time.Hour, t, TestRedisUri(), false)
	defer clean()

	dbh := app.DBM.DBH(app.Logger)

	for i := 0; i < 10; i++ {
		_, err := dbh.Exec(`INSERT INTO queue (id) VALUES ($1)`, uuid.New())
		expect.Nil(err)
	}
	var count int

	expect.Nil(dbh.QueryRow(`SELECT COUNT(1) FROM queue`).Scan(&count))
	expect.Equal(10, count)

	wg := new(sync.WaitGroup)
	wg.Add(1)

	go func() {
		defer wg.Done()
		app.Run()
	}()

	go func() {
		for i := 0; i < 20; i++ {
			h, err := app.RM.Handler(app.Logger)
			expect.Nil(err)
			_, err = h.Do("PUBLISH", "queue", "do")
			expect.Nil(err)
			time.Sleep(50 * time.Millisecond)
		}
		done <- struct{}{}
	}()

	// waiting for workers
	for i := 0; i < 10; i++ {
		<-finished
	}

	<-done

	app.Stop()
	wg.Wait()

	expect.Nil(dbh.QueryRow(`SELECT COUNT(1) FROM queue`).Scan(&count))
	expect.Zero(count)

	expect.Len(processed, 10)
	// хз почему иногда 21 срабатывание (
	expect.True(len(triggered) >= 20 && len(triggered) < 22)

	expect.True(listenerExited)
	expect.True(poolExited)
}

func Skat_Greedy_Scenario(t *testing.T) {
	hooks.Reset()
	expect := assert.New(t)

	var listenerExited, poolExited bool
	finished := make(chan int, 100)
	done := make(chan struct{})
	triggered := make(chan struct{}, 1000)
	processed := make(chan struct{}, 1000)

	hooks.OnReadFanOut = func() {
		triggered <- struct{}{}
	}
	hooks.OnJobPerform = func() {
		processed <- struct{}{}
	}
	hooks.OnListenerExit = func() {
		listenerExited = true
	}
	hooks.OnPoolExit = func() {
		poolExited = true
	}
	hooks.OnTaskFinish = func(i int) {
		finished <- i
	}

	app, clean := newSkat(time.Hour, t, TestRedisUri(), true)
	defer clean()

	dbh := app.DBM.DBH(app.Logger)

	for i := 0; i < 10; i++ {
		_, err := dbh.Exec(`INSERT INTO queue (id) VALUES ($1)`, uuid.New())
		expect.Nil(err)
	}
	var count int

	expect.Nil(dbh.QueryRow(`SELECT COUNT(1) FROM queue`).Scan(&count))
	expect.Equal(10, count)

	wg := new(sync.WaitGroup)
	wg.Add(1)

	go func() {
		defer wg.Done()
		app.Run()
	}()

	go func() {
		h, err := app.RM.Handler(app.Logger)
		expect.Nil(err)
		_, err = h.Do("PUBLISH", "queue", "do")
		expect.Nil(err)
		done <- struct{}{}
	}()

	// waiting for workers
	for i := 0; i < 10; i++ {
		<-finished
	}

	<-done
	app.Stop()

	wg.Wait()

	expect.Nil(dbh.QueryRow(`SELECT COUNT(1) FROM queue`).Scan(&count))
	expect.Zero(count)

	expect.True(listenerExited)
	expect.True(poolExited)
	expect.Len(processed, 10)
	// хз почему иногда 2 срабатывания (
	expect.True(len(triggered) >= 1 && len(triggered) < 3)
}

func newSkat(dur time.Duration, t *testing.T, redisURL string, greedy bool) (*App, func()) {
	conf := &config.Config{
		RedisURL:       redisURL,
		WorkerPoolSize: 100,
		Duration:       dur,
		Greedy:         greedy,
	}

	logger := test.NewLogger(t)

	user, pass, uri := TestPostgresCreds()
	template := "postgres://" + user + ":" + pass + uri + "%s?sslmode=disable"
	dbName := DB_NAME + os.Getenv("BUILD_NUMBER")

	err := CreateOrResetDB(template, dbName, user, SCHEMA_PATH, SCHEMA_NAME)
	if err != nil {
		t.Fatal(err)
	}

	clean := func() {
		DropDB(fmt.Sprintf(template, "postgres"), dbName)
	}

	db, err := sql.Open("postgres", fmt.Sprintf(template+"&search_path=%s", dbName, SCHEMA_NAME))
	if err != nil {
		log.Fatal(err)
	}

	db.SetMaxOpenConns(24)
	db.SetMaxIdleConns(16)

	dbm := mrs.NewDBM(db)

	rm := engine.NewRedis(conf.RedisURL)

	return &App{
		DBM:    dbm,
		RM:     rm,
		Config: conf,
		Logger: logger,
		done:   make(chan struct{}),
	}, clean
}
