package jobs

import (
	"skat/queue"
	"skat/registry"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestManifest(t *testing.T) {
	expect := assert.New(t)
	expect.NotNil(registry.Find(&queue.Data{Type: "base"}))
}
