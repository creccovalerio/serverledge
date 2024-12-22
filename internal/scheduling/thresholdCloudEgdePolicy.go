package scheduling

import (
	"fmt"
	"log"
	"runtime"

	"github.com/grussorusso/serverledge/internal/config"
	"github.com/grussorusso/serverledge/internal/node"
)

/* ThresholdCloudEdgePolicy supports Cloud/Edge offloading. Offload is executed  *
 * if the amount of local available resources is lower than a certain threshold. */
type ThresholdCloudEdgePolicy struct{}

func (p *ThresholdCloudEdgePolicy) Init() {
}

func (p *ThresholdCloudEdgePolicy) OnCompletion(_ *scheduledRequest) {
}

func (p *ThresholdCloudEdgePolicy) OnArrival(r *scheduledRequest) {

	availableCores := runtime.NumCPU()
	totAvailableMem := int64(config.GetInt(config.POOL_MEMORY_MB, 1024))
	totAvailableCPUs := config.GetFloat(config.POOL_CPUS, float64(availableCores))
	cpuThreshold := totAvailableCPUs * 0.20
	memThreshold := float64(totAvailableMem) * 0.30

	fmt.Println("------------------------------------------------------------")
	fmt.Printf("Scheduled function: %s with policy: THRESHOLD_CLOUD/EDGE\n", r.Fun.Name)
	fmt.Println("------------------------------------------------------------")
	fmt.Println("Available amount of CPU: ", node.Resources.AvailableCPUs)
	fmt.Println("Tot amount of CPU: ", totAvailableCPUs)
	fmt.Println("Available amount of MemMB: ", node.Resources.AvailableMemMB)
	fmt.Printf("Tot amount of MemMB: %d - threshold: %f\n", totAvailableMem, memThreshold)
	fmt.Println("------------------------------------------------------------")
	fmt.Println("")

	if r.CanDoOffloading {
		/* checking if the scheduled function has been already containerized (resources already allocated)*/
		containerID, err := node.AcquireWarmContainer(r.Fun)
		if err == nil {
			log.Printf("Using a warm container for: %v\n", r)
			execLocally(r, containerID, true)
			return
		} else {
			if node.Resources.AvailableCPUs >= cpuThreshold &&
				float64(node.Resources.AvailableMemMB) >= memThreshold {
				handleColdStart(r)
				return
			} else {
				/* if function offloading flag is active and at least one of the previous *
				* performance condition are met: schedule decision -> Function Offload   */
				fmt.Println("Scheduling function on the cloud...")
				handleCloudOffload(r)
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
