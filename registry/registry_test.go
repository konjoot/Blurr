package registry

import (
	"github.com/konjoot/blurr/jobs/base"
	"github.com/konjoot/blurr/jobs/interfaces"
	"github.com/konjoot/blurr/queue"
	"testing"
)

func TestRegistry(t *testing.T) {
	Add("base", base.New)

	var job interfaces.Performer

	job = Find(&queue.Data{Type: "base"})
	if job == nil {
		t.Error(isNil, job)
	}

	job = Find(&queue.Data{Type: "unsup"})
	if job != nil {
		t.Errorf(notEqual, nil, job)
	}
}
