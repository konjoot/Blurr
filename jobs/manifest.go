// Манифест заданий.
//
// чтобы задание было доступно в реестре заданий нужно добавить строчку в init() функцию, как это сделано для базового задания
package jobs

import (
	"skat/registry"

	"skat/jobs/base"
)

func init() {
	registry.Add("base", base.New)
}
