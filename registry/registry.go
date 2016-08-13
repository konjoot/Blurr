// Реестр заданий, сюда попадают все задания указанные в манифесте (jobs/manifest.go)
package registry

import (
	"github.com/konjoot/blurr/jobs/interfaces"
	"github.com/konjoot/blurr/queue"
)

var knownJobs map[string]Constructor

type Constructor func(*queue.Data) interfaces.Performer

func init() {
	knownJobs = make(map[string]Constructor)
}

func Find(data *queue.Data) interfaces.Performer {
	if job, ok := knownJobs[data.Type]; ok {
		return job(data)
	}
	return nil
}

func Add(name string, constructor Constructor) {
	knownJobs[name] = constructor
}
