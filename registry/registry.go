// Реестр заданий, сюда попадают все задания указанные в манифесте (jobs/manifest.go)
package registry

import (
	"skat/jobs/interfaces"
	"skat/queue"
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
