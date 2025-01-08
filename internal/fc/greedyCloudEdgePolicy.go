package fc

import (
	"fmt"
	"reflect"
	"runtime"
	"time"

	"github.com/grussorusso/serverledge/internal/config"
	"github.com/grussorusso/serverledge/internal/function"
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

func findMaxRespTime(respTimes []float64) float64 {
	// check empty slice
	if len(respTimes) == 0 {
		return 0
	}
	max := respTimes[0]
	for _, value := range respTimes[1:] {
		if value > max {
			max = value
		}
	}

	return max
}

func computeResidualLocalAndRemoteExecutionRespTime(r *scheduledFcRequest, nodeId DagNodeId, localRespTime float64, remoteRespTime float64) (float64, float64) {
	node, ok := r.Fc.Workflow.Find(nodeId)
	if !ok {
		return 0.0, 0.0
	}

	switch n := node.(type) {

	case *StartNode:
		fmt.Println("PROCESSING START")
		localRespTime, remoteRespTime := computeResidualLocalAndRemoteExecutionRespTime(r, n.GetNext()[0], localRespTime, remoteRespTime)
		fmt.Println("RET START: ", localRespTime, remoteRespTime)
		return localRespTime, remoteRespTime
	case *SimpleNode:
		fmt.Println("PROCESSING SIMPLE")
		funct, ok := function.GetFunction(n.Func)
		if !ok {
			return 0.0, 0.0
		}
		localRespTime += currentDataGcep.AvgFunDurationTime[funct.Name]
		remoteRespTime += currentDataGcep.AvgFunRemoteDurationTime[funct.Name]
		fmt.Println("INC TIME SIMPLE: ", localRespTime, remoteRespTime)
		localRespTime, remoteRespTime := computeResidualLocalAndRemoteExecutionRespTime(r, n.GetNext()[0], localRespTime, remoteRespTime)
		fmt.Println("RET SIMPLE: ", localRespTime, remoteRespTime)
		return localRespTime, remoteRespTime
	case *PassNode, *WaitNode, *SucceedNode, *FailNode:
		fmt.Println("PROCESSING ", reflect.TypeOf(n))
		localRespTime, remoteRespTime := computeResidualLocalAndRemoteExecutionRespTime(r, n.GetNext()[0], localRespTime, remoteRespTime)
		fmt.Printf("RET %T: %f %f", reflect.TypeOf(n), localRespTime, remoteRespTime)
		return localRespTime, remoteRespTime
	case *ChoiceNode:
		tmpLocalRespTime := 0.0
		tmpRemoteRespTime := 0.0
		for i, alternative := range n.Alternatives {
			branchId := fmt.Sprintf("%s_branch_%d", string(n.GetId()), i)
			branchProb := currentDataGcep.NoBranchInvocations[branchId] / currentDataGcep.NoChoiceNodeInvocations[string(n.GetId())]
			fmt.Printf("NODE: %s NoCHOICENODEINVK: %f - NoBRANCHINVK: %f --> BRANCH PROB: %f\n", string(n.GetId()), currentDataGcep.NoChoiceNodeInvocations[string(n.GetId())], currentDataGcep.NoBranchInvocations[branchId], branchProb)
			subLocalRespTime, subRemoteRespTime := computeResidualLocalAndRemoteExecutionRespTime(r, alternative, 0.0, 0.0)
			fmt.Println("PRE INC TIME CHOICE: ", subLocalRespTime, subRemoteRespTime)
			tmpLocalRespTime += branchProb * subLocalRespTime
			tmpRemoteRespTime += branchProb * subRemoteRespTime
			fmt.Println("INC TIME CHOICE: ", tmpLocalRespTime, tmpRemoteRespTime)
		}
		localRespTime += tmpLocalRespTime
		remoteRespTime += tmpRemoteRespTime
		fmt.Println("RET FIN CHOICE: ", localRespTime, remoteRespTime)
		return localRespTime, remoteRespTime
	case *FanOutNode:
		var localParallelRespTime []float64
		var remoteParallelRespTime []float64
		for _, parallelBranch := range n.GetNext() {
			fmt.Println("PROCESSING FANOUT")
			localTime, remoteTime := computeResidualLocalAndRemoteExecutionRespTime(r, parallelBranch, 0.0, 0.0)
			fmt.Println("PARALLEL TIMES: ", localTime, remoteTime)
			localParallelRespTime = append(localParallelRespTime, localTime)
			remoteParallelRespTime = append(remoteParallelRespTime, remoteTime)
			fmt.Println("SLICE: ", localParallelRespTime, remoteParallelRespTime)
		}
		fmt.Println("PRE INC TIME FANOUT: ", localRespTime, localRespTime)
		localRespTime += findMaxRespTime(localParallelRespTime)
		remoteRespTime += findMaxRespTime(remoteParallelRespTime)
		fmt.Println("RET FIN FANOUT: ", localRespTime, remoteRespTime)
		return localRespTime, remoteRespTime
	case *FanInNode:
		fmt.Println("PROCESSING FANIN")
		localRespTime, remoteRespTime := computeResidualLocalAndRemoteExecutionRespTime(r, n.GetNext()[0], localRespTime, remoteRespTime)
		fmt.Println("RET FANIN: ", localRespTime, remoteRespTime)
		return localRespTime, remoteRespTime
	case *EndNode:
		fmt.Println("PROCESSING END")
		fmt.Println("RET END: ", localRespTime, remoteRespTime)
		return localRespTime, remoteRespTime
	}

	return localRespTime, remoteRespTime

}

func findCurrentNode(r *scheduledFcRequest) ([]DagNodeId, error) {
	currentNodes, err := r.progress.NextNodes()
	if err != nil {
		return nil, fmt.Errorf("error in retriving NextNodes()")
	}
	return currentNodes, nil
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
	currentNodes, err := findCurrentNode(r)
	if err == nil {
		if len(currentNodes) > 1 {
			/* handling case of parallels nodes */
			var localParallelRespTime []float64
			var remoteParallelRespTime []float64
			/* cycle to find the parallel node with the max (local & remote) resp time */
			for i, _ := range currentNodes {
				fmt.Println("**************************************  START ESTIMATION FROM NODE: ", currentNodes[i])
				estimatedLocalResidualRespTime, estimatedRemoteResidualRespTime = computeResidualLocalAndRemoteExecutionRespTime(r, currentNodes[i], localRespTime, remoteRespTime)
				fmt.Println("**************************************  END ESTIMATION")
				localParallelRespTime = append(localParallelRespTime, estimatedLocalResidualRespTime)
				remoteParallelRespTime = append(remoteParallelRespTime, estimatedRemoteResidualRespTime)
				fmt.Println("**************************************  LISTS: ", localParallelRespTime, remoteParallelRespTime)

			}
			/* find the max (local&remote) resp time to pass to policy */
			estimatedLocalResidualRespTime = findMaxRespTime(localParallelRespTime)
			estimatedRemoteResidualRespTime = findMaxRespTime(remoteParallelRespTime)
			fmt.Println("**************************************  INFOS: ", estimatedLocalResidualRespTime, estimatedRemoteResidualRespTime)

		} else {
			/* handling all the other kind of nodes */
			fmt.Println("**************************************  START ESTIMATION FROM NODE: ", currentNodes[0])
			estimatedLocalResidualRespTime, estimatedRemoteResidualRespTime = computeResidualLocalAndRemoteExecutionRespTime(r, currentNodes[0], localRespTime, remoteRespTime)
			fmt.Println("**************************************  END ESTIMATION")
			fmt.Println("**************************************  INFOS: ", estimatedLocalResidualRespTime, estimatedRemoteResidualRespTime)
		}

	} else {
		return
	}

	/* Decide to execute the workflow to a cloud node if:                *
	 *	- workflow offloading is active;                                 *
	 *  - remote execution (which includes Ttransfer&Treturn) lasts less *
	 *  - than the local execution                                       */
	if r.CanDoFcOffloading && !r.IsInProfilingMode &&
		(tTransfer+estimatedRemoteResidualRespTime+tReturn <= estimatedLocalResidualRespTime) {
		/* if fc offloading flag is active and the previous            *
		 * performance condition are met: schedule decision -> Offload */
		fmt.Println("Scheduling the remaining part of the workflow on the cloud...")
		handleCloudOffload(r)
		return
	} else if node.Resources.AvailableCPUs >= 0 &&
		float64(node.Resources.AvailableMemMB) >= 0 {
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
