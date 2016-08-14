package jobs

import (
	"github.com/konjoot/blurr/jobs/interfaces"
	"github.com/konjoot/blurr/queue"
	"github.com/konjoot/blurr/registry"
	"testing"
)

func TestManifest(t *testing.T) {
	var task interfaces.Performer

	if task = registry.Find(&queue.Data{Type: "base"}); task == nil {
		t.Errorf(isEqual, nil, task)
	}
	if task = registry.Find(&queue.Data{Type: "undefined"}); task != nil {
		t.Errorf(notEqual, nil, task)
	}
}
