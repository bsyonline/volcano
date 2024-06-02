package foo

import (
	"fmt"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
)

const PluginName = "foo"

type fooPlugin struct {
	// Arguments given for the plugin
	pluginArguments framework.Arguments
}

func New(arguments framework.Arguments) framework.Plugin {
	return &fooPlugin{pluginArguments: arguments}
}

func (foo *fooPlugin) Name() string {
	return PluginName
}

func (foo *fooPlugin) OnSessionOpen(ssn *framework.Session) {
	loggingFn := func(jobs []*api.JobInfo) {
		for _, job := range jobs {
			if job.IsPending() {
				fmt.Printf("%v.%v is pending", job.Namespace, job.Name)
			}
		}
	}
	ssn.AddPrintFns(PluginName, loggingFn)
}

func (foo *fooPlugin) OnSessionClose(ssn *framework.Session) {

}
