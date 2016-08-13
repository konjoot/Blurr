package interfaces

import (
	"github.com/konjoot/blurr/queue"

	"golang.org/x/net/context"
)

type Performer interface {
	Perform(context.Context) error
}

type Finder func(*queue.Data) Performer
