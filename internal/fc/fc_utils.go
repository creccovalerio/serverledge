package fc

import (
	"fmt"

	"github.com/grussorusso/serverledge/internal/function"
)

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

func computeResidualLocalAndRemoteExecutionRespTime(r *scheduledFcRequest, nodeId DagNodeId, localRespTime float64, remoteRespTime float64, metrics ReturnedOutputData) (float64, float64) {
	node, ok := r.Fc.Workflow.Find(nodeId)
	if !ok {
		return 0.0, 0.0
	}

	switch n := node.(type) {

	case *StartNode:
		localRespTime, remoteRespTime := computeResidualLocalAndRemoteExecutionRespTime(r, n.GetNext()[0], localRespTime, remoteRespTime, metrics)
		return localRespTime, remoteRespTime
	case *SimpleNode:
		funct, ok := function.GetFunction(n.Func)
		if !ok {
			return 0.0, 0.0
		}
		localRespTime += metrics.AvgFunDurationTime[funct.Name]
		remoteRespTime += metrics.AvgFunRemoteDurationTime[funct.Name]
		localRespTime, remoteRespTime := computeResidualLocalAndRemoteExecutionRespTime(r, n.GetNext()[0], localRespTime, remoteRespTime, metrics)
		return localRespTime, remoteRespTime
	case *PassNode, *WaitNode, *SucceedNode, *FailNode:
		localRespTime, remoteRespTime := computeResidualLocalAndRemoteExecutionRespTime(r, n.GetNext()[0], localRespTime, remoteRespTime, metrics)
		return localRespTime, remoteRespTime
	case *ChoiceNode:
		tmpLocalRespTime := 0.0
		tmpRemoteRespTime := 0.0
		for i, alternative := range n.Alternatives {
			branchId := fmt.Sprintf("%s_branch_%d", string(n.GetId()), i)
			branchProb := metrics.NoBranchInvocations[branchId] / metrics.NoChoiceNodeInvocations[string(n.GetId())]
			subLocalRespTime, subRemoteRespTime := computeResidualLocalAndRemoteExecutionRespTime(r, alternative, 0.0, 0.0, metrics)
			tmpLocalRespTime += branchProb * subLocalRespTime
			tmpRemoteRespTime += branchProb * subRemoteRespTime
		}
		localRespTime += tmpLocalRespTime
		remoteRespTime += tmpRemoteRespTime
		return localRespTime, remoteRespTime
	case *FanOutNode:
		var localParallelRespTime []float64
		var remoteParallelRespTime []float64
		for _, parallelBranch := range n.GetNext() {
			localTime, remoteTime := computeResidualLocalAndRemoteExecutionRespTime(r, parallelBranch, 0.0, 0.0, metrics)
			localParallelRespTime = append(localParallelRespTime, localTime)
			remoteParallelRespTime = append(remoteParallelRespTime, remoteTime)
		}
		localRespTime += findMaxRespTime(localParallelRespTime)
		remoteRespTime += findMaxRespTime(remoteParallelRespTime)
		return localRespTime, remoteRespTime
	case *FanInNode:
		localRespTime, remoteRespTime := computeResidualLocalAndRemoteExecutionRespTime(r, n.GetNext()[0], localRespTime, remoteRespTime, metrics)
		return localRespTime, remoteRespTime
	case *EndNode:
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
