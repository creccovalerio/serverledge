package scheduling

import (
	"fmt"
	"log"

	"github.com/grussorusso/serverledge/internal/node"
)

/* CloudOnlyPolicy can be used on Edge nodes to always offload on cloud. *
 * If offloading is disabled, the request is dropped                     */
type CloudOnlyPolicy struct{}

func (p *CloudOnlyPolicy) Init() {
}

func (p *CloudOnlyPolicy) OnCompletion(_ *scheduledRequest) {
}

func (p *CloudOnlyPolicy) OnArrival(r *scheduledRequest) {

	fmt.Println("----------------------------------------------------")
	fmt.Printf("Scheduled function: %s with policy: CLOUD_ONLY\n", r.Fun.Name)
	fmt.Println("----------------------------------------------------")

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

	if r.CanDoOffloading || r.IsInProfileLatencyMode {
		handleCloudOffload(r)
	} else {
		dropRequest(r)
	}
}
