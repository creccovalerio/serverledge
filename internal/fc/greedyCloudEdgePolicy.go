package fc

import (
	"fmt"
	"runtime"
	"time"

	"github.com/grussorusso/serverledge/internal/config"
	"github.com/grussorusso/serverledge/internal/node"
)

var currentDataPcep ReturnedOutputData

type GreedyCloudEdgePolicy struct{}

func (p *GreedyCloudEdgePolicy) SubmitInfos(data ReturnedOutputData) {
	timestamp := time.Now()
	dataMap[timestamp] = data //adding actual data to historical data
	currentDataPcep = data

	fmt.Println("------------------------------------------")
	fmt.Println("Timestamp Key: ", timestamp)
	fmt.Println("Metrics: ", dataMap[timestamp])
	fmt.Println("------------------------------------------")

	fmt.Println("")
}

func (p *GreedyCloudEdgePolicy) Init() {
}

func (p *GreedyCloudEdgePolicy) OnCompletion(_ *scheduledFcRequest) {

}

func (p *GreedyCloudEdgePolicy) OnArrival(r *scheduledFcRequest) {

	availableCores := runtime.NumCPU()
	totAvailableMem := int64(config.GetInt(config.POOL_MEMORY_MB, 1024))
	totAvailableCPUs := config.GetFloat(config.POOL_CPUS, float64(availableCores))
	cpuTreshold := totAvailableCPUs * (0.20)
	memTreshold := float64(totAvailableMem) * (0.30)

	fmt.Println("------------------------------------------")
	fmt.Println("Scheduled workflow: ", r.Fc.Name)
	fmt.Printf("Avg Fc [%s] Response Time: %f\n", r.Fc.Name, currentDataPcep.AvgFcRespTime[r.Fc.Name])
	fmt.Printf("Actual Fc [%s] Response Time: %f\n", r.Fc.Name, r.ExecReport.ResponseTime)

	for _, fname := range r.Fc.Workflow.GetUniqueDagFunctions() {
		fmt.Printf("Avg Function [%s] Cold Start Time: %f\n", fname, currentDataPcep.AvgTotalColdStartsTime[fname])
	}

	for _, fname := range r.Fc.Workflow.GetUniqueDagFunctions() {
		fmt.Printf("Avg Function [%s] Response Time: %f\n", fname, currentDataPcep.AvgFunDurationTime[fname])
	}

	for _, fname := range r.Fc.Workflow.GetUniqueDagFunctions() {
		fmt.Printf("Avg Output Function [%s] Size: %f\n", fname, currentDataPcep.AvgOutputFunSize[fname])
	}

	fmt.Println("Available amount of CPU: ", node.Resources.AvailableCPUs)
	fmt.Println("Tot amount of CPU: ", totAvailableCPUs)
	fmt.Println("Available amount of MemMB: ", node.Resources.AvailableMemMB)
	fmt.Println("Tot amount of MemMB: ", totAvailableMem)
	fmt.Println("------------------------------------------")

	fmt.Println("")

	/* Decide to execute the workflow to a cloud node if:                        *
	 *	- workflow offloading is active;                                         *
	 *  - The user specified fcMaxRespTime is less than the profilefc avg 		 *
	 *    response time                                                          *
	 *	- current fc Response time is greater than the 95th percentile of the    *
	 *	  profiled avg fc response time;                                         *
	 *	- the amount of cpu & memory is less than a specified threshold;         */
	if r.CanDoFcOffloading && r.Iteration > 0 &&
		(node.Resources.AvailableCPUs <= cpuTreshold ||
			float64(node.Resources.AvailableMemMB) <= memTreshold) {
		/* if fc offloading flag is active and at least one of the previous *
		 * performance condition are met: schedule decision -> Offload      */
		fmt.Println("Scheduling the remaining part of the workflow on the cloud...")
		handleCloudOffload(r)
		return
	} else if node.Resources.AvailableCPUs >= cpuTreshold &&
		float64(node.Resources.AvailableMemMB) >= memTreshold {
		/* if fc offloading flag is NOT active and the previous performance *
		 * condition are met: fc schedule decision -> Exec locally          */
		fmt.Println("Scheduling locally...")
		handleExecuteLocal(r)
		return
	} else {
		/* if fc offloading flag is NOT active and threre are not enough *
		 * resources: fc schedule decision -> Drop request               */
		fmt.Println("Dropping...")
		dropRequest(r)
		return
	}

}
