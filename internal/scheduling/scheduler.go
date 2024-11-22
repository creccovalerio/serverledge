package scheduling

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"runtime"
	"time"

	"github.com/grussorusso/serverledge/internal/node"
	"github.com/grussorusso/serverledge/internal/telemetry"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/grussorusso/serverledge/internal/config"

	"github.com/grussorusso/serverledge/internal/container"
	"github.com/grussorusso/serverledge/internal/function"
)

var requests chan *scheduledRequest

var parentCtx context.Context

// var compositionRequests chan *scheduledCompositionRequest // watch out for circular import!!!
var completions chan *completion

var remoteServerUrl string
var offloadingClient *http.Client

func Run(p Policy) {
	requests = make(chan *scheduledRequest, 500)
	completions = make(chan *completion, 500)

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
				meter := otel.Meter(os.Getenv("OTEL_SERVICE_NAME"))
				m, err := telemetry.NewHistogramMetric(meter, "Function.duration", "Duration of a function")
				if err != nil {
					panic(err)
				}
				m.Record(
					c.scheduledRequest.Ctx,
					c.ExecReport.Duration,
					metric.WithAttributes(attribute.String("functInvocationCounter", c.Fun.Name)))

				mtr, err := telemetry.NewHistogramMetric(meter, "FunctionOutput.size", "Size of the function output")
				if err != nil {
					panic(err)
				}
				mtr.Record(
					c.scheduledRequest.Ctx,
					float64(len([]byte(c.ExecReport.Result))),
					metric.WithAttributes(attribute.String("functionSizeHistogram", c.Fun.Name)))

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
		err = Offload(r, schedDecision.remoteHost)
		if err != nil {
			return err
		}
	} else {
		err = Execute(schedDecision.contID, &schedRequest, r.IsInComposition) // executing request
		if err != nil {
			return err
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

	var m metric.Float64Histogram
	var err error
	var start time.Time
	if telemetry.DefaultTracer != nil {
		trace.SpanFromContext(r.Ctx).AddEvent("Container init start")
	}

	if telemetry.MetricsEnabled {
		meter := otel.Meter(os.Getenv("OTEL_SERVICE_NAME"))
		m, err = telemetry.NewHistogramMetric(meter, "ColdStart.duration", "Duration of a cold start")
		if err != nil {
			panic(err)
		}
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
			duration := time.Since(start)
			m.Record(
				r.Ctx,
				duration.Seconds(),
				metric.WithAttributes(attribute.String("functColdStartHistogram", r.Fun.Name)))
		}
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
}

func handleCloudOffload(r *scheduledRequest) {
	cloudAddress := config.GetString(config.CLOUD_URL, "")
	handleOffload(r, cloudAddress)
}
