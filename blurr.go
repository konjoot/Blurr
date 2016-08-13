package blurr

import (
	"os"
	"os/signal"
	"time"

	"github.com/konjoot/blurr/hooks"
	"github.com/konjoot/blurr/pool"
	"github.com/konjoot/blurr/task"
)

// New the blurr's constructor
func New() Blurr {
	return &app{
		cfg:  newCfg(),
		done: make(chan struct{}),
	}
}

// blurr's interface
type Blurr interface {
	Run()
	Stop()
}

// blurr inner structure
type app struct {
	// config
	cfg *cfg
	// semaphore of the application's shutdown
	done chan struct{}
}

// Run starts the blurr
func (app *app) Run() {

	// pool initialization
	pool, err := pool.New(app.cfg.pool, app.cfg.greedy)
	if err != nil {
		// log point
		return
	}
	defer pool.Wait()

	// subscribe to the system sygnals
	interrupt := make(chan os.Signal)
	signal.Notify(interrupt, os.Interrupt)

	// tiker initialization
	ticker := time.NewTicker(app.cfg.dur)
	defer ticker.Stop()

	// when it's false - event loop stops.
	keepWorking := true
	// blurr's event loop
	for keepWorking {
		select {
		case <-ticker.C: // process jobs from ticker
			send(pool.In)
		case <-app.done: // shutdown by code
			keepWorking = false
		case <-interrupt: // shutdown by a system signal
			keepWorking = false
		}
	}

	// stop working
	pool.Cancel()
}

// blurr's poweroff button
func (app *app) Stop() {
	close(app.done)
}

// emits new tasks to the pool
func send(in chan task.Performer) {
	select {
	case in <- task.New():
		hooks.OnProcess()
	default:
	}
}
