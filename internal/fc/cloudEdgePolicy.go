package fc

import (
	"fmt"
	"runtime"
	"time"

	"github.com/grussorusso/serverledge/internal/config"
	"github.com/grussorusso/serverledge/internal/node"
)

// CloudEdgePolicy supports only Edge-Cloud Offloading. Executes locally first,
// but if no resources are available and offload is enabled offloads the request to a cloud node.
// If no resources are available and offloading is disabled, drops the request.
type CloudEdgePolicy struct{}

var currentDataCep ReturnedOutputData

func (p *CloudEdgePolicy) SubmitInfos(data ReturnedOutputData) {
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

func (p *CloudEdgePolicy) OnCompletion(_ *scheduledFcRequest) {

}

func (p *CloudEdgePolicy) OnArrival(r *scheduledFcRequest) {

	availableCores := runtime.NumCPU()
	totAvailableMem := int64(config.GetInt(config.POOL_MEMORY_MB, 1024))
	totAvailableCPUs := config.GetFloat(config.POOL_CPUS, float64(availableCores))
	cpuTreshold := totAvailableCPUs * (20 / 100)
	memTreshold := totAvailableMem * (30 / 100)

	fmt.Println("------------------------------------------")
	fmt.Println("SCHEDULED REQUEST: ", r.Fc.Name)
	fmt.Println("Avg Cold Start Time: ", currentDataCep.AvgTotalColdStartsTime)
	fmt.Println("Avg Fc Response Time: ", r.Fc.Name, currentDataCep.AvgFcRespTime[r.Fc.Name])
	fmt.Println("Actual Fc Response Time: ", r.ExecReport.ResponseTime)

	for _, fname := range r.Fc.Workflow.GetUniqueDagFunctions() {
		fmt.Println("Avg Function Response Time: ", fname, currentDataCep.AvgFunDurationTime[fname])
	}
	for _, fname := range r.Fc.Workflow.GetUniqueDagFunctions() {
		fmt.Println("Avg Output Function Size: ", fname, currentDataCep.AvgOutputFunSize[fname])
	}

	fmt.Println("Available amount of CPU: ", node.Resources.AvailableCPUs)
	fmt.Println("Available amount of MemMB: ", node.Resources.AvailableMemMB)
	fmt.Println("Tot amount of CPU: ", totAvailableCPUs)
	fmt.Println("Tot amount of MemMB: ", totAvailableMem)
	fmt.Println("------------------------------------------")

	fmt.Println("")

	/*if r.CanDoFcOffloading && r.Iteration >= 3 {
		handleCloudOffload(r)
	} else {
		handleExecuteLocal(r)
	}*/
	/*
		if r.CanDoFcOffloading {
			nextNodes, err := r.progress.NextNodes()
			if err != nil {
				fmt.Println("Error in retriving NextNodes()")
			} else {
				n, ok := r.Fc.Workflow.Find(nextNodes[0])
				fmt.Printf("Node type: %T\n", n)
				if ok {
					switch node := n.(type) {
					case *SimpleNode:
						fmt.Println("EXEC SIMPLE")
						funct, ok := function.GetFunction(node.Func)
						if ok {
							fmt.Println("Scheduling Node with func: ", funct)
						}
					}
				}
			}
		}*/

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
		fmt.Println("Scheduling the remaining part of the fc on the cloud...")
		handleCloudOffload(r)
	} else {
		handleExecuteLocal(r)
	}

	/*
		if r.CanDoFcOffloading && r.Iteration >= 3 {
			handleCloudOffload(r)
		} else {
			handleExecuteLocal(r)
		}*/

}
