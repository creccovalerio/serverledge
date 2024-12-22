package scheduling

import (
	"fmt"
	"log"

	"github.com/grussorusso/serverledge/internal/node"
)

/* EdgePolicy supports only local execution. It tries to acquire a warm container   *
 * or to handle the cold start in order to execute a function locally. If the local *
 * execution is not possible, the request will be dropped						    */
type EdgeOnlyPolicy struct{}

func (p *EdgeOnlyPolicy) Init() {
}

func (p *EdgeOnlyPolicy) OnCompletion(_ *scheduledRequest) {
}

func (p *EdgeOnlyPolicy) OnArrival(r *scheduledRequest) {

	fmt.Println("----------------------------------------------------")
	fmt.Printf("Scheduled function: %s with policy: EDGE_ONLY\n", r.Fun.Name)
	fmt.Println("----------------------------------------------------")
	containerID, err := node.AcquireWarmContainer(r.Fun)
	if err == nil {
		log.Printf("Using a warm container for: %v\n", r)
		execLocally(r, containerID, true)
		return
	} else if handleColdStart(r) {
		return
	}

	dropRequest(r)
}
