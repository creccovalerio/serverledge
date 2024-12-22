package fc

import (
	"fmt"
	"runtime"
	"time"

	"github.com/grussorusso/serverledge/internal/config"
	"github.com/grussorusso/serverledge/internal/function"
	"github.com/grussorusso/serverledge/internal/node"
)

/* ThresholdCloudEdgePolicy supports Cloud/Edge offloading. Offload is executed *
 * if the amount of available resources is lower than a certain threshold.      */
type ThresholdDynCloudEdgePolicy struct{}

func (p *ThresholdDynCloudEdgePolicy) SubmitInfos(data ReturnedOutputData) {
	timestamp := time.Now()
	dataMap[timestamp] = data //adding actual data to historical data

	fmt.Println("------------------------------------------")
	fmt.Println("Timestamp Key: ", timestamp)
	fmt.Println("Metrics: ", dataMap[timestamp])
	fmt.Println("------------------------------------------")

	fmt.Println("")
}

func (p *ThresholdDynCloudEdgePolicy) Init() {
}

func (p *ThresholdDynCloudEdgePolicy) OnCompletion(_ *scheduledFcRequest) {
}

func (p *ThresholdDynCloudEdgePolicy) OnArrival(r *scheduledFcRequest) {

	availableCores := runtime.NumCPU()
	totAvailableMem := int64(config.GetInt(config.POOL_MEMORY_MB, 1024))
	totAvailableCPUs := config.GetFloat(config.POOL_CPUS, float64(availableCores))
	cpuTreshold := totAvailableCPUs * (0.20)
	memTreshold := float64(totAvailableMem) * (0.30)

	fmt.Println("---------------------------------------------------------------")
	fmt.Printf("Scheduled workflow: %s with policy: THRESHOLD_CLOUD/EDGE\n", r.Fc.Name)
	fmt.Println("---------------------------------------------------------------")
	fmt.Println("Available amount of CPU: ", node.Resources.AvailableCPUs)
	fmt.Println("Tot amount of CPU: ", totAvailableCPUs)
	fmt.Println("Available amount of MemMB: ", node.Resources.AvailableMemMB)
	fmt.Println("Tot amount of MemMB: ", totAvailableMem)
	fmt.Println("---------------------------------------------------------------")

	fmt.Println("")

	/*If in profiling mode -> execute the workflow remotely or locally in order to collect execution metrics */
	if r.IsInProfilingMode {
		if r.CanDoFcOffloading {
			/* if Profiling flag is active: schedule decision -> Offload */
			fmt.Println("Scheduling the entire workflow on the cloud...")
			handleCloudOffload(r)
			return
		} else {
			/* if Profiling flag is NOT active: schedule decision -> Local execution */
			fmt.Println("Scheduling the entire workflow on the locally...")
			handleExecuteLocal(r)
			return
		}
	}

	/* Decide to execute the workflow to a cloud node if:                        *
	 *	- workflow offloading is active;                                         *                                         *
	 *	- the amount of cpu & memory is less than a specified threshold;         */
	if r.CanDoFcOffloading && !r.IsInProfilingMode {
		nextNodes, err := r.progress.NextNodes()
		if err != nil {
			fmt.Println("Error in retriving NextNodes()")
		} else {
			n, ok := r.Fc.Workflow.Find(nextNodes[0])
			if ok {
				switch nd := n.(type) {
				case *SimpleNode:
					funct, ok := function.GetFunction(nd.Func)
					if !ok {
						fmt.Println("Error in GetFunction()")
					}
					created := node.HasWarmContainers(funct)
					if !created {
						fmt.Println("Container not initialized for function: ", funct)
						if node.Resources.AvailableCPUs < cpuTreshold ||
							float64(node.Resources.AvailableMemMB) < memTreshold {
							/* if fc offloading flag is active and at least one of the previous *
							* performance condition are met: schedule decision -> Offload      */
							fmt.Println("Scheduling the remaining part of the workflow on the cloud...")
							handleCloudOffload(r)
							return
						}
					}
				}
			}
		}

		fmt.Printf("There are warm containers for: %v\n", r)
		handleExecuteLocal(r)
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
