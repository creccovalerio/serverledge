package fc

import (
	"fmt"
	"runtime"
	"time"

	"github.com/grussorusso/serverledge/internal/config"
	"github.com/grussorusso/serverledge/internal/node"
)

/* EdgeOnlyPolicy does always local execution. If the local *
 * execution is not possible, the request will be dropped   */
type EdgeOnlyPolicy struct{}

func (p *EdgeOnlyPolicy) SubmitInfos(data ReturnedQueryMetrics) {
	timestamp := time.Now()
	dataMap[timestamp] = data //adding actual data to historical data

	fmt.Println("------------------------------------------")
	fmt.Println("Timestamp Key: ", timestamp)
	fmt.Println("Metrics: ", dataMap[timestamp])
	fmt.Println("------------------------------------------")

	fmt.Println("")
}

func (p *EdgeOnlyPolicy) Init() {
}

func (p *EdgeOnlyPolicy) OnCompletion(_ *scheduledFcRequest) {
}

func (p *EdgeOnlyPolicy) OnArrival(r *scheduledFcRequest) {
	availableCores := runtime.NumCPU()
	totAvailableMem := int64(config.GetInt(config.POOL_MEMORY_MB, 1024))
	totAvailableCPUs := config.GetFloat(config.POOL_CPUS, float64(availableCores))

	fmt.Println("--------------------------------------------")
	fmt.Printf("Scheduled workflow: %s with policy: EDGE_ONLY\n", r.Fc.Name)
	fmt.Println("--------------------------------------------")
	fmt.Println("Available amount of CPU: ", node.Resources.AvailableCPUs)
	fmt.Println("Tot amount of CPU: ", totAvailableCPUs)
	fmt.Println("Available amount of MemMB: ", node.Resources.AvailableMemMB)
	fmt.Println("Tot amount of MemMB: ", totAvailableMem)
	fmt.Println("--------------------------------------------")

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

	if !r.IsInProfilingMode &&
		(node.Resources.AvailableCPUs > totAvailableCPUs*0.05 &&
			float64(node.Resources.AvailableMemMB) > float64(totAvailableMem)*0.05) {
		/* if fc offloading flag is NOT active and there are enough *
		 * resources: fc schedule decision -> Exec locally          */
		fmt.Println("Scheduling locally...")
		handleExecuteLocal(r)
		return
	} else {
		/* if fc offloading flag is NOT active and threre are not *
		 * enough resources: fc schedule decision -> Drop request */
		fmt.Println("Dropping...")
		dropRequest(r)
		return
	}

}
