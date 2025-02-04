package fc

import (
	"fmt"
	"time"

	"github.com/grussorusso/serverledge/internal/node"
	"github.com/grussorusso/serverledge/utils"
)

var currentDataDcep ReturnedOutputData

/* DeadlineCloudEdgePolicy execute Cloud offloading only if the         *
 * user specified FcMaxRespTime is less than the profiled avgFcRespTime */
type DeadlineCloudEdgePolicy struct{}

func (p *DeadlineCloudEdgePolicy) SubmitInfos(data ReturnedOutputData) {
	timestamp := time.Now()
	dataMap[timestamp] = data //adding actual data to historical data
	currentDataDcep = data

	fmt.Println("------------------------------------------")
	fmt.Println("Timestamp Key: ", timestamp)
	fmt.Println("Metrics: ", dataMap[timestamp])
	fmt.Println("------------------------------------------")

	fmt.Println("")
}

func (p *DeadlineCloudEdgePolicy) Init() {
}

func (p *DeadlineCloudEdgePolicy) OnCompletion(_ *scheduledFcRequest) {
}

func (p *DeadlineCloudEdgePolicy) OnArrival(r *scheduledFcRequest) {

	key := fmt.Sprintf("%s_%s", r.Fc.Name, utils.FormatParams(r.Params))
	fmt.Println("------------------------------------------------------------")
	fmt.Printf("Scheduled workflow: %s with policy: DEADLINE_CLOUD/EDGE\n", r.Fc.Name)
	fmt.Println("------------------------------------------------------------")
	fmt.Printf("Avg Fc [%s] Response Time: %f\n", r.Fc.Name, currentDataDcep.AvgFcRespTime[r.Fc.Name])
	fmt.Printf("Avg Fc [%s] Response Time Per Input %s: %f\n", r.Fc.Name, utils.FormatParams(r.Params), currentDataDcep.AvgFcRespTimePerInput[key])
	fmt.Printf("Max RespTime admitted for the Fc [%s]: %f\n", r.Fc.Name, r.QoSMaxFcRespT)
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

	/* Decide to execute the workflow to a cloud node if:   *
	 *	- workflow offloading is active;                    *
	 *  - The user specified fcMaxRespTime is less than the *
	 *    profiled fc avg response time                     */
	if r.CanDoFcOffloading && !r.IsInProfilingMode &&
		(r.QoSMaxFcRespT > 0 && r.QoSMaxFcRespT <= currentDataDcep.AvgFcRespTimePerInput[key]) {
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
