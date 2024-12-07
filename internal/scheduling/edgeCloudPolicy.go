package scheduling

import (
	"fmt"
	"time"

	"github.com/grussorusso/serverledge/internal/node"
)

var dataMap map[time.Time]ReturnedFunctionOutputData
var currentDataCep ReturnedFunctionOutputData

// CloudEdgePolicy supports only Edge-Cloud Offloading. Executes locally first,
// but if no resources are available and offload is enabled offloads the request to a cloud node.
// If no resources are available and offloading is disabled, drops the request.
type CloudEdgePolicy struct{}

func (p *CloudEdgePolicy) SubmitInfos(data ReturnedFunctionOutputData) {
	timestamp := time.Now()
	dataMap[timestamp] = data
	currentDataCep = data
	//dataMap[timestamp] = data //adding actual data to historical data
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

func (p *CloudEdgePolicy) OnCompletion(_ *scheduledRequest) {

}

func (p *CloudEdgePolicy) OnArrival(r *scheduledRequest) {
	containerID, err := node.AcquireWarmContainer(r.Fun)
	if err == nil {
		execLocally(r, containerID, true)
	} else if handleColdStart(r) {
		return
	} else if r.CanDoOffloading {
		handleCloudOffload(r)
	} else {
		dropRequest(r)
	}
}
