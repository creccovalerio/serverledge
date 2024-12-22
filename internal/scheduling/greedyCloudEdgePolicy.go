package scheduling

import (
	"fmt"
	"log"
	"time"

	"github.com/grussorusso/serverledge/internal/node"
)

var currentDataGcep ReturnedFunctionOutputData

// GreedyCloudEdgePolicy supports Cloud Offloading only if the local execution time
// is greater of the remote execution time, otherwise it execute the function locally.
// If not enough resources are available locally and offload is enabled, drops the request.
// If not enough resources are available and offloading is disabled, drops the request.
type GreedyCloudEdgePolicy struct{}

func (p *GreedyCloudEdgePolicy) SubmitInfos(data ReturnedFunctionOutputData) {
	timestamp := time.Now()
	dataMap[timestamp] = data
	currentDataGcep = data
	//dataMap[timestamp] = data //adding actual data to historical data
	fmt.Println("------------------------------------------")
	fmt.Println("Func Timestamp Key: ", timestamp)
	fmt.Println("Func Metrics: ", dataMap[timestamp])
	fmt.Println("------------------------------------------")
	fmt.Println("")
}

func (p *GreedyCloudEdgePolicy) Init() {
}

func (p *GreedyCloudEdgePolicy) OnCompletion(_ *scheduledRequest) {

}

func (p *GreedyCloudEdgePolicy) OnArrival(r *scheduledRequest) {

	fmt.Println("------------------------------------------------------------")
	fmt.Printf("Scheduled function: %s with policy: GREEDY_CLOUD/EDGE\n", r.Fun.Name)
	fmt.Println("------------------------------------------------------------")
	fmt.Println("")

	if r.IsInProfilingMode {
		/* if local profiling is active, bypass function scheduler policy  *
		 * during the profiling in order to obtain local execution metrics */
		containerID, err := node.AcquireWarmContainer(r.Fun)
		if err == nil {
			log.Printf("Using a warm container for: %v\n", r)
			execLocally(r, containerID, true)
			return
		} else if handleColdStart(r) {
			return
		} else {
			dropRequest(r)
			return
		}
	}

	if r.CanDoOffloading {
		if currentDataGcep.AvgFunRemoteDurationTime[r.Fun.Name] <= currentDataGcep.AvgFunDurationTime[r.Fun.Name] {
			/* if function offloading flag is active and AvgRemoteFunctionDurationTime is <= than *
			 * LocalAvgFunctionDurationTime: sched decision -> Function Offload                   */
			fmt.Println("Scheduling function on the cloud...")
			handleCloudOffload(r)
			return
		} else {
			containerID, err := node.AcquireWarmContainer(r.Fun)
			if err == nil {
				log.Printf("Using a warm container for: %v\n", r)
				execLocally(r, containerID, true)
				return
			} else if handleColdStart(r) {
				return
			} else {
				dropRequest(r)
				return
			}
		}

	} else {
		containerID, err := node.AcquireWarmContainer(r.Fun)
		if err == nil {
			log.Printf("Using a warm container for: %v\n", r)
			execLocally(r, containerID, true)
			return
		} else if handleColdStart(r) {
			return
		}
	}

	dropRequest(r)
}
