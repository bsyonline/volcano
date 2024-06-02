package foo

import (
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
)

type Action struct{}

func New() *Action {
	return &Action{}
}

func (foo *Action) Name() string {
	return "foo"
}

func (foo *Action) Initialize() {}

func (foo *Action) Execute(ssn *framework.Session) {
	jobs := make([]*api.JobInfo, 0)
	for _, job := range ssn.Jobs {
		jobs = append(jobs, job)
	}
	ssn.PrintJobInfo(jobs)
}

func (foo *Action) UnInitialize() {}
