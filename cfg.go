package blurr

import (
	"flag"
	"time"
)

// newCfg parses flags and initizes config (*cfg) instance.
func newCfg() *cfg {
	cfg := &cfg{}

	flag.IntVar(&cfg.pool, "pool", 32,
		"count of the workers in pool (default: 32)")

	flag.BoolVar(&cfg.greedy, "greedy", true,
		"enable greedy mode (default: true)")

	flag.DurationVar(&cfg.dur, "duration", time.Minute,
		"pool's heartbeat duration")

	flag.Parse()

	return cfg
}

// the config :)
type cfg struct {
	// count of the workers in pool
	pool int
	// greedy mode enable/disable
	greedy bool
	// pool's heartbeat duration
	dur time.Duration
}
