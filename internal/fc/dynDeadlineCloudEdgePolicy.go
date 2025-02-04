package fc

import (
	"fmt"
	"time"

	"github.com/grussorusso/serverledge/internal/node"
	"github.com/grussorusso/serverledge/utils"
)

var currentDataDdcep ReturnedOutputData

/* DynDeadlineCloudEdgePolicy execute Cloud offloading only if the   *
 * current RespTime at the nth step of the workflow is less than the *
 * profiled avgFcRespTime */
type DynDeadlineCloudEdgePolicy struct{}

func (p *DynDeadlineCloudEdgePolicy) SubmitInfos(data ReturnedOutputData) {
	timestamp := time.Now()
	dataMap[timestamp] = data //adding actual data to historical data
	currentDataDdcep = data

	fmt.Println("------------------------------------------")
	fmt.Println("Timestamp Key: ", timestamp)
	fmt.Println("Metrics: ", dataMap[timestamp])
	fmt.Println("------------------------------------------")

	fmt.Println("")
}

func (p *DynDeadlineCloudEdgePolicy) Init() {
}

func (p *DynDeadlineCloudEdgePolicy) OnCompletion(_ *scheduledFcRequest) {
}

func (p *DynDeadlineCloudEdgePolicy) OnArrival(r *scheduledFcRequest) {

	key := fmt.Sprintf("%s_%s", r.Fc.Name, utils.FormatParams(r.Params))
	fmt.Println("------------------------------------------------------------")
	fmt.Printf("Scheduled workflow: %s with policy: DYN DEADLINE_CLOUD/EDGE\n", r.Fc.Name)
	fmt.Println("------------------------------------------------------------")
	fmt.Printf("Avg Fc [%s] Response Time: %f\n", r.Fc.Name, currentDataDdcep.AvgFcRespTime[r.Fc.Name])
	fmt.Printf("Avg Fc [%s] Response Time Per Input %s: %f\n", r.Fc.Name, utils.FormatParams(r.Params), currentDataDdcep.AvgFcRespTimePerInput[key])
	fmt.Printf("Current Fc [%s] RespTime: %f\n", r.Fc.Name, r.ExecReport.ResponseTime)
	fmt.Println("------------------------------------------------------------")

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

	/* Decide to execute the workflow to a cloud node if:                *
	 *	- workflow offloading is active;                                 *
	 *  - The current Fc response time (at the nth iteration of the dag) *
	 *  - is less than the profiled fc avg response time                 */
	if r.CanDoFcOffloading && !r.IsInProfilingMode &&
		(r.ExecReport.ResponseTime > currentDataDdcep.AvgFcRespTimePerInput[key]*0.95) {
		/* if fc offloading flag is active and the user specified FcMaxRespTime *
		 * is smaller than the AvgFcRespTime: schedule decision -> Offload      */
		fmt.Println("Scheduling the remaining part of the workflow on the cloud...")
		handleCloudOffload(r)
		return
	} else if node.Resources.AvailableCPUs >= 0 &&
		float64(node.Resources.AvailableMemMB) >= 0 {
		/* if fc offloading flag is NOT active and there are enough *
		 * resuorces: fc schedule decision -> Exec locally          */
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
