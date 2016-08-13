package registry

import (
	"github.com/konjoot/blurr/jobs/base"
	"github.com/konjoot/blurr/queue"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRegistry(t *testing.T) {
	expect := assert.New(t)

	Add("base", base.New)

	expect.NotNil(Find(&queue.Data{Type: "base"}))
	expect.Nil(Find(&queue.Data{Type: "unsup"}))
}
