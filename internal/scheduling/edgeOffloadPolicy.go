package scheduling

import (
	"log"

	"github.com/grussorusso/serverledge/internal/node"
)

// EdgePolicy supports only Edge-Edge offloading. Always does offloading to an edge node if enabled. When offloading is not enabled executes the request locally.
type EdgeOffloadPolicy struct{}

func (p *EdgeOffloadPolicy) Init() {
}

func (p *EdgeOffloadPolicy) OnCompletion(_ *scheduledRequest) {

}

func (p *EdgeOffloadPolicy) OnArrival(r *scheduledRequest) {
	if r.CanDoOffloading {
		url := pickEdgeNodeForOffloading(r)
		if url != "" {
			handleOffload(r, url)
			return
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
