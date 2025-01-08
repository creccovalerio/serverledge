package scheduling

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"runtime"
	"time"

	"github.com/grussorusso/serverledge/internal/node"
	"github.com/grussorusso/serverledge/internal/telemetry"
	"github.com/grussorusso/serverledge/utils"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/grussorusso/serverledge/internal/config"

	"github.com/grussorusso/serverledge/internal/container"
	"github.com/grussorusso/serverledge/internal/function"
)

var requests chan *scheduledRequest
var dataMap map[time.Time]ReturnedFunctionOutputData

var parentCtx context.Context

// var compositionRequests chan *scheduledCompositionRequest // watch out for circular import!!!
var completions chan *completion

var remoteServerUrl string
var offloadingClient *http.Client

func Run(p Policy) {
	requests = make(chan *scheduledRequest, 500)
	completions = make(chan *completion, 500)
	dataMap = make(map[time.Time]ReturnedFunctionOutputData)

	// initialize Resources resources
	availableCores := runtime.NumCPU()
	node.Resources.AvailableMemMB = int64(config.GetInt(config.POOL_MEMORY_MB, 1024))
	node.Resources.AvailableCPUs = config.GetFloat(config.POOL_CPUS, float64(availableCores))
	node.Resources.ContainerPools = make(map[string]*node.ContainerPool)
	log.Printf("Current resources: %v\n", &node.Resources)

	container.InitDockerContainerFactory()

	//janitor periodically remove expired warm container
	node.GetJanitorInstance()

	tr := &http.Transport{
		MaxIdleConns:        2500,
		MaxIdleConnsPerHost: 2500,
		MaxConnsPerHost:     0,
		IdleConnTimeout:     30 * time.Minute,
	}
	offloadingClient = &http.Client{Transport: tr}

	// initialize scheduling policy
	p.Init()

	remoteServerUrl = config.GetString(config.CLOUD_URL, "")

	log.Println("Scheduler started.")

	var r *scheduledRequest
	var c *completion
	for {
		select {
		case r = <-requests: // receive request
			go p.OnArrival(r)
		case c = <-completions:
			node.ReleaseContainer(c.contID, c.Fun)
			p.OnCompletion(c.scheduledRequest)
			if telemetry.MetricsEnabled {
				utils.CreateAndRecordNewHistogramMetric("Function.duration", "Duration of a function", c.scheduledRequest.Ctx, c.ExecReport.Duration, "functInvocationDuration", c.Fun.Name)
				utils.CreateAndRecordNewHistogramMetric("FunctionOutput.size", "Size of the function output", c.scheduledRequest.Ctx, float64(len([]byte(c.ExecReport.Result))), "functionSizeHistogram", c.Fun.Name)
			}
		}
	}

}

func SetParentCtx(ctx context.Context) {
	parentCtx = ctx
}

// SubmitRequest submits a newly arrived request for scheduling and execution
func SubmitRequest(r *function.Request) error {
	schedRequest := scheduledRequest{
		Request:         r,
		decisionChannel: make(chan schedDecision, 1)}
	requests <- &schedRequest // send request

	// Tracing
	if telemetry.DefaultTracer != nil {
		childCtx, childSpan := telemetry.DefaultTracer.Start(parentCtx, "invocation")
		r.Ctx = childCtx
		defer childSpan.End()
		childSpan.SetAttributes(attribute.String("function", r.Fun.Name))
	}

	if telemetry.DefaultTracer != nil {
		trace.SpanFromContext(r.Ctx).AddEvent("Scheduling start")
	}

	// wait on channel for scheduling action
	schedDecision, ok := <-schedRequest.decisionChannel
	if !ok {
		return fmt.Errorf("could not schedule the request")
	}
	//log.Printf("[%s] Scheduling decision: %v", r, schedDecision)

	if telemetry.DefaultTracer != nil {
		trace.SpanFromContext(r.Ctx).AddEvent("Scheduling complete")
	}

	var err error
	if schedDecision.action == DROP {
		//log.Printf("[%s] Dropping request", r)
		return node.OutOfResourcesErr
	} else if schedDecision.action == EXEC_REMOTE {
		//log.Printf("Offloading request\n")
		if telemetry.DefaultTracer != nil {
			trace.SpanFromContext(r.Ctx).AddEvent("Offload function start")
		}

		err = Offload(r, schedDecision.remoteHost)
		if err != nil {
			return err
		}

		if telemetry.DefaultTracer != nil {
			trace.SpanFromContext(r.Ctx).AddEvent("Offload function complete")
		}

	} else {
		if telemetry.DefaultTracer != nil {
			trace.SpanFromContext(r.Ctx).AddEvent("Execute function start")
		}

		err = Execute(schedDecision.contID, &schedRequest, r.IsInComposition) // executing request
		if err != nil {
			return err
		}

		if telemetry.DefaultTracer != nil {
			trace.SpanFromContext(r.Ctx).AddEvent("Execute function complete")
		}
	}
	return nil
}

// SubmitAsyncRequest submits a newly arrived async request for scheduling and execution
func SubmitAsyncRequest(r *function.Request) {
	schedRequest := scheduledRequest{
		Request:         r,
		decisionChannel: make(chan schedDecision, 1)}
	requests <- &schedRequest // send async request

	// wait on channel for scheduling action
	schedDecision, ok := <-schedRequest.decisionChannel
	if !ok {
		PublishAsyncResponse(r.Id(), function.Response{Success: false})
		return
	}

	var err error
	if schedDecision.action == DROP {
		PublishAsyncResponse(r.Id(), function.Response{Success: false})
	} else if schedDecision.action == EXEC_REMOTE {
		//log.Printf("Offloading request\n")
		err = OffloadAsync(r, schedDecision.remoteHost)
		if err != nil {
			PublishAsyncResponse(r.Id(), function.Response{Success: false})
		}
	} else {
		err = Execute(schedDecision.contID, &schedRequest, r.IsInComposition) // executing async request
		if err != nil {
			PublishAsyncResponse(r.Id(), function.Response{Success: false})
		}
		PublishAsyncResponse(r.Id(), function.Response{Success: true, ExecutionReport: r.ExecReport})
	}
}

func handleColdStart(r *scheduledRequest) (isSuccess bool) {

	var err error
	var start time.Time
	var duration time.Duration
	if telemetry.DefaultTracer != nil {
		trace.SpanFromContext(r.Ctx).AddEvent("Container init start")
	}

	if telemetry.MetricsEnabled {
		start = time.Now()
	}

	newContainer, err := node.NewContainer(r.Fun)
	if errors.Is(err, node.OutOfResourcesErr) {
		return false
	} else if err != nil {
		log.Printf("Cold start failed: %v\n", err)
		return false
	} else {
		if telemetry.DefaultTracer != nil {
			trace.SpanFromContext(r.Ctx).AddEvent("Container init complete")
		}

		if telemetry.MetricsEnabled {
			duration = time.Since(start)
			utils.CreateAndRecordNewHistogramMetric("ColdStart.duration", "Duration of a cold start", r.Ctx, duration.Seconds(), "functColdStartHistogram", r.Fun.Name)
		}
		r.ExecReport.ColdStartTime = duration.Seconds()
		execLocally(r, newContainer, false)
		return true
	}
}

func dropRequest(r *scheduledRequest) {
	r.decisionChannel <- schedDecision{action: DROP}
}

func execLocally(r *scheduledRequest, c container.ContainerID, warmStart bool) {
	initTime := time.Now().Sub(r.Arrival).Seconds()
	r.ExecReport.InitTime = initTime
	r.ExecReport.IsWarmStart = warmStart
	r.ExecReport.SchedAction = "Execute_local"

	decision := schedDecision{action: EXEC_LOCAL, contID: c}
	r.decisionChannel <- decision
}

func handleOffload(r *scheduledRequest, serverHost string) {
	r.CanDoOffloading = false // the next server can't offload this request
	r.decisionChannel <- schedDecision{
		action:     EXEC_REMOTE,
		contID:     "",
		remoteHost: serverHost,
	}
	r.ExecReport.SchedAction = "Offloaded"
}

func handleCloudOffload(r *scheduledRequest) {
	cloudAddress := config.GetString(config.CLOUD_URL, "")
	handleOffload(r, cloudAddress)
}
