package fc

import (
	"fmt"
	"runtime"
	"time"

	"github.com/grussorusso/serverledge/internal/config"
	"github.com/grussorusso/serverledge/internal/node"
)

var currentDataCep ReturnedOutputData

type CloudEdgePolicy struct{}

func (p *CloudEdgePolicy) SubmitInfos(data ReturnedOutputData) {
	timestamp := time.Now()
	dataMap[timestamp] = data //adding actual data to historical data
	currentDataCep = data

	fmt.Println("------------------------------------------")
	fmt.Println("Timestamp Key: ", timestamp)
	fmt.Println("Metrics: ", dataMap[timestamp])
	fmt.Println("------------------------------------------")

	fmt.Println("")
}

func (p *CloudEdgePolicy) Init() {
}

func (p *CloudEdgePolicy) OnCompletion(_ *scheduledFcRequest) {

}

func (p *CloudEdgePolicy) OnArrival(r *scheduledFcRequest) {

	availableCores := runtime.NumCPU()
	totAvailableMem := int64(config.GetInt(config.POOL_MEMORY_MB, 1024))
	totAvailableCPUs := config.GetFloat(config.POOL_CPUS, float64(availableCores))
	cpuTreshold := totAvailableCPUs * (20 / 100)
	memTreshold := totAvailableMem * (30 / 100)

	fmt.Println("------------------------------------------")
	fmt.Println("Scheduled workflow: ", r.Fc.Name)
	fmt.Println("Avg Cold Start Time: ", currentDataCep.AvgTotalColdStartsTime)
	fmt.Printf("Avg Fc [%s] Response Time: %f\n", r.Fc.Name, currentDataCep.AvgFcRespTime[r.Fc.Name])
	fmt.Printf("Actual Fc [%s] Response Time: %f\n", r.Fc.Name, r.ExecReport.ResponseTime)

	for _, fname := range r.Fc.Workflow.GetUniqueDagFunctions() {
		fmt.Printf("Avg Function [%s] Response Time: %f\n", fname, currentDataCep.AvgFunDurationTime[fname])
	}
	for _, fname := range r.Fc.Workflow.GetUniqueDagFunctions() {
		fmt.Printf("Avg Output Function [%s] Size: %f\n", fname, currentDataCep.AvgOutputFunSize[fname])
	}

	fmt.Println("Available amount of CPU: ", node.Resources.AvailableCPUs)
	fmt.Println("Tot amount of CPU: ", totAvailableCPUs)
	fmt.Println("Available amount of MemMB: ", node.Resources.AvailableMemMB)
	fmt.Println("Tot amount of MemMB: ", totAvailableMem)
	fmt.Println("------------------------------------------")

	fmt.Println("")

	/* Decide to execute the workflow into a cloud node if:                      *
	 *	- workflow offloading is active;                                         *
	 *	- current fc Response time is greater than the 95th percentile of the    *
	 *	  avg fc response time                                                   *
	 *	- the amount of cpu & memory is less than a fixed threshold              *
	 *  - the fc avg response time is less than the user specified fcMaxRespTime */
	if r.CanDoFcOffloading && r.Iteration > 0 &&
		(r.QoSMaxFcRespT > currentDataCep.AvgFcRespTime[r.Fc.Name]*0.95 ||
			r.ExecReport.ResponseTime > currentDataCep.AvgFcRespTime[r.Fc.Name] ||
			r.ExecReport.ResponseTime > (currentDataCep.AvgFcRespTime[r.Fc.Name]*0.95) ||
			node.Resources.AvailableCPUs <= cpuTreshold ||
			node.Resources.AvailableMemMB <= memTreshold) {

		/* if fc offloading flag is active and at least one of the previous *
		 * performance condition are met: schedule decision -> Offload      */
		fmt.Println("Scheduling the remaining part of the workflow on the cloud...")
		handleCloudOffload(r)
		return
	} else if node.Resources.AvailableCPUs >= cpuTreshold &&
		node.Resources.AvailableMemMB >= memTreshold {
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
