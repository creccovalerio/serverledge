package fc

import (
	"fmt"
)

// CloudEdgePolicy supports only Edge-Cloud Offloading. Executes locally first,
// but if no resources are available and offload is enabled offloads the request to a cloud node.
// If no resources are available and offloading is disabled, drops the request.
type CloudEdgePolicy struct{}

func SubmitInfos(data ReturnedOutputData) {
	//dataMetrics = data // actual retrieved data
	dataMap[data.Timestamp] = data //adding actual data to historical data
	for key := range dataMap {
		fmt.Println("------------------------------------------")
		fmt.Println("Timestamp Key: ", key)
		fmt.Println("Metrics: ", dataMap[key])
		fmt.Println("------------------------------------------")
	}
	fmt.Println("")
}

func (p *CloudEdgePolicy) Init() {
}

func (p *CloudEdgePolicy) OnCompletion(_ *scheduledFcRequest) {

}

func (p *CloudEdgePolicy) OnArrival(r *scheduledFcRequest) {

	if r.CanDoFcOffloading && r.Iteration >= 3 {
		handleCloudOffload(r)
	} else {
		handleExecuteLocal(r)
	}
}
