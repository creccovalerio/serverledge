package fc

import (
	"fmt"
	"runtime"
	"time"

	"github.com/grussorusso/serverledge/internal/config"
	"github.com/grussorusso/serverledge/internal/node"
)

var currentDataGcep ReturnedOutputData

type GreedyCloudEdgePolicy struct{}

func (p *GreedyCloudEdgePolicy) SubmitInfos(data ReturnedOutputData) {
	timestamp := time.Now()
	dataMap[timestamp] = data //adding actual data to historical data
	currentDataGcep = data

	fmt.Println("------------------------------------------")
	fmt.Println("Timestamp Key: ", timestamp)
	fmt.Println("Metrics: ", dataMap[timestamp])
	fmt.Println("")
	fmt.Println("------------------------------------------")

	fmt.Println("")
}

func (p *GreedyCloudEdgePolicy) Init() {
}

func (p *GreedyCloudEdgePolicy) OnCompletion(_ *scheduledFcRequest) {
}

func (p *GreedyCloudEdgePolicy) OnArrival(r *scheduledFcRequest) {

	var localRespTime float64 = 0.0
	var remoteRespTime float64 = 0.0
	var estimatedLocalResidualRespTime = 0.0
	var estimatedRemoteResidualRespTime = 0.0
	availableCores := runtime.NumCPU()
	totAvailableMem := int64(config.GetInt(config.POOL_MEMORY_MB, 1024))
	totAvailableCPUs := config.GetFloat(config.POOL_CPUS, float64(availableCores))

	fmt.Println("------------------------------------------")
	fmt.Println("Scheduled workflow: ", r.Fc.Name)

	for _, fname := range r.Fc.Workflow.GetUniqueDagFunctions() {
		fmt.Printf("Avg Function [%s] Response Time: %f\n", fname, currentDataGcep.AvgFunDurationTime[fname])
	}

	fmt.Println("Available amount of CPU: ", node.Resources.AvailableCPUs)
	fmt.Println("Tot amount of CPU: ", totAvailableCPUs)
	fmt.Println("Available amount of MemMB: ", node.Resources.AvailableMemMB)
	fmt.Println("Tot amount of MemMB: ", totAvailableMem)
	fmt.Println("------------------------------------------")

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

	tTransfer := currentDataGcep.AvgFcTTransferTime[r.Fc.Name]
	tReturn := currentDataGcep.AvgFcTReturnTime[r.Fc.Name]
	tSaving := currentDataGcep.AvgSavingInfosTime[r.Fc.Name]

	/* compute time estimations only for the first step of the workflow acting as a static policy */
	if r.Iteration == 0 {
		currentNodes, err := findCurrentNode(r)
		if err == nil {
			if len(currentNodes) > 1 {
				/* handling case of parallels nodes */
				var localParallelRespTime []float64
				var remoteParallelRespTime []float64
				/* cycle to find the parallel node with the max (local & remote) resp time */
				for i := range currentNodes {
					fmt.Println("**************************************  Start estimation from node: ", currentNodes[i])
					estimatedLocalResidualRespTime, estimatedRemoteResidualRespTime = computeResidualLocalAndRemoteExecutionRespTime(r, currentNodes[i], localRespTime, remoteRespTime, currentDataGcep)
					fmt.Println("**************************************  End estimation")
					localParallelRespTime = append(localParallelRespTime, estimatedLocalResidualRespTime)
					remoteParallelRespTime = append(remoteParallelRespTime, estimatedRemoteResidualRespTime)
					fmt.Println("**************************************  Lists: ", localParallelRespTime, remoteParallelRespTime, tTransfer, tReturn)

				}
				/* find the max (local&remote) resp time to pass to policy */
				estimatedLocalResidualRespTime = findMaxRespTime(localParallelRespTime)
				estimatedRemoteResidualRespTime = findMaxRespTime(remoteParallelRespTime)
				fmt.Println("**************************************  Estimated Times: ", estimatedLocalResidualRespTime, estimatedRemoteResidualRespTime, tTransfer, tReturn, tSaving)

			} else {
				/* handling all the other kind of nodes */
				fmt.Println("**************************************  Start estimation from node: ", currentNodes[0])
				estimatedLocalResidualRespTime, estimatedRemoteResidualRespTime = computeResidualLocalAndRemoteExecutionRespTime(r, currentNodes[0], localRespTime, remoteRespTime, currentDataGcep)
				fmt.Println("**************************************  End estimation")
				fmt.Println("**************************************  Estimated Times: ", estimatedLocalResidualRespTime, estimatedRemoteResidualRespTime, tTransfer, tReturn)
			}

		} else {
			return
		}
	}

	/* Decide to execute the workflow to a cloud node if:                  *
	 *  - workflow offloading is active;                                   *
	 *  - remote execution (which includes Ttransfer&Treturn) ends before  *
	 *  - than the local execution                                         */
	if r.CanDoFcOffloading && !r.IsInProfilingMode && r.Iteration == 0 &&
		(tSaving+tTransfer+estimatedRemoteResidualRespTime+tReturn <= estimatedLocalResidualRespTime) {
		/* if fc offloading flag is active and the previous            *
		 * performance condition are met: schedule decision -> Offload */
		fmt.Println("Scheduling the remaining part of the workflow on the cloud...")
		handleCloudOffload(r)
		return
	} else if node.Resources.AvailableCPUs >= totAvailableCPUs*0.05 &&
		float64(node.Resources.AvailableMemMB) >= float64(totAvailableMem)*0.05 {
		/* if fc offloading flag is NOT active and the previous performance *
		 * conditions are met: fc schedule decision -> Exec locally         */
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
