package fc

import "fmt"

// CloudEdgePolicy supports only Edge-Cloud Offloading. Executes locally first,
// but if no resources are available and offload is enabled offloads the request to a cloud node.
// If no resources are available and offloading is disabled, drops the request.
type CloudEdgePolicy struct{}

func (p *CloudEdgePolicy) Init() {
}

func (p *CloudEdgePolicy) OnCompletion(_ *scheduledFcRequest) {

}

func (p *CloudEdgePolicy) OnArrival(r *scheduledFcRequest) {
	fmt.Println("CANDOOFF - ITER: ", r.CanDoFcOffloading, r.Iteration)
	if r.CanDoFcOffloading && r.Iteration == 3 {
		handleCloudOffload(r)
	} else {
		handleExecuteLocal(r)
	}
}
