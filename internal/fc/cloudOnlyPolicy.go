package fc

import (
	"fmt"
	"time"
)

type CloudOnlyPolicy struct{}

func (p *CloudOnlyPolicy) SubmitInfos(data ReturnedOutputData) {
	timestamp := time.Now()
	dataMap[timestamp] = data //adding actual data to historical data

	fmt.Println("------------------------------------------")
	fmt.Println("Timestamp Key: ", timestamp)
	fmt.Println("Metrics: ", dataMap[timestamp])
	fmt.Println("------------------------------------------")

	fmt.Println("")
}

/* CloudOnlyPolicy execute always Cloud offloading. If the cloud offload *
 * is not possible, the request will be dropped                          */
func (p *CloudOnlyPolicy) Init() {
}

func (p *CloudOnlyPolicy) OnCompletion(_ *scheduledFcRequest) {
}

func (p *CloudOnlyPolicy) OnArrival(r *scheduledFcRequest) {

	fmt.Println("---------------------------------------------------")
	fmt.Printf("Scheduled workflow: %s with policy: CLOUD_ONLY\n", r.Fc.Name)
	fmt.Println("---------------------------------------------------")

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
	 *	- workflow offloading is active;                                         */
	if r.CanDoFcOffloading {
		/* if fc offloading flag is active: schedule decision -> Offload */
		fmt.Println("Scheduling the remaining part of the workflow on the cloud...")
		handleCloudOffload(r)
		return
	} else {
		/* if fc offloading flag is NOT active: fc schedule decision -> Drop request */
		fmt.Println("Dropping...")
		dropRequest(r)
		return
	}

}
