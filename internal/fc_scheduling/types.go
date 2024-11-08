package fc_scheduling

import "github.com/grussorusso/serverledge/internal/fc"

type scheduledFcRequest struct {
	//*function.Request /* Non function.Request ma Fc.Request*/
	*fc.CompositionRequest
	fcDecisionChannel chan fcSchedDecision
	priority          float64
}

type completion struct {
	*scheduledFcRequest
	//contID container.ContainerID
}

type fcSchedDecision struct {
	action     action
	remoteHost string
}

type action int64

const (
	DROP                  action = 0
	EXEC_LOCAL                   = 1
	EXEC_REMOTE                  = 2
	BEST_EFFORT_EXECUTION        = 3
)
