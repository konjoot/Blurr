package jobs

import (
	"github.com/konjoot/blurr/queue"
	"github.com/konjoot/blurr/registry"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestManifest(t *testing.T) {
	expect := assert.New(t)
	expect.NotNil(registry.Find(&queue.Data{Type: "base"}))
	expect.Nil(registry.Find(&queue.Data{Type: "undefined"}))
}
