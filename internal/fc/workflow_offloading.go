package fc

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/grussorusso/serverledge/internal/client"
	"github.com/grussorusso/serverledge/internal/function"
	"github.com/grussorusso/serverledge/internal/node"
	"github.com/grussorusso/serverledge/internal/telemetry"
	"go.opentelemetry.io/otel/trace"
)

const SCHED_ACTION_OFFLOAD = "O"

// TODO: offload the entire node when is cloud only
func WorkflowOffload(r *CompositionRequest, serverUrl string, reports map[ExecutionReportId]*function.ExecutionReport) (CompositionExecutionReport, bool, error) {

	exe_reports := make(map[string]*function.ExecutionReport)
	for key, report := range reports {
		exe_reports[string(key)] = report
	}

	start := time.Now()
	tTransferStart := float64(start.Unix()) + float64(start.Nanosecond())/1e9

	request := client.OffloadedCompositionInvocationRequest{
		ReqId:           r.Id(),
		Params:          r.Params,
		Reports:         exe_reports,
		CanDoOffloading: false, // blocking another possible offload of the same request on the cloud node
		Ttransfer:       tTransferStart,
	}
	invocationBody, err := json.Marshal(request)
	if err != nil {
		log.Print(invocationBody)
		return CompositionExecutionReport{}, true, err
	}

	if telemetry.DefaultTracer != nil {
		trace.SpanFromContext(r.Ctx).AddEvent("Offload Post start")
	}

	resp, err := offloadingClient.Post(serverUrl+"/offload/"+r.Fc.Name, "application/json",
		bytes.NewBuffer(invocationBody))

	if err != nil {
		log.Print(err)
		return CompositionExecutionReport{}, true, err
	}

	respArrivalTime := time.Now()
	receivedTime := float64(respArrivalTime.Unix()) + float64(respArrivalTime.Nanosecond())/1e9

	if telemetry.DefaultTracer != nil {
		trace.SpanFromContext(r.Ctx).AddEvent("Offload Post complete")
	}

	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusTooManyRequests {
			return CompositionExecutionReport{}, true, node.OutOfResourcesErr
		}
		return CompositionExecutionReport{}, true, fmt.Errorf("\nRemote returned: %v", resp.StatusCode)
	}

	if telemetry.DefaultTracer != nil {
		trace.SpanFromContext(r.Ctx).AddEvent("Process Offload Post response start")
	}

	var responseExecutionReport CompositionExecutionReport
	var response CompositionResponse
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			fmt.Printf("Error while closing offload response body: %s\n", err)
		}
	}(resp.Body)

	now := time.Now()

	body, _ := io.ReadAll(resp.Body)
	if err = json.Unmarshal(body, &response); err != nil {
		return CompositionExecutionReport{}, true, err
	}

	responseExecutionReport.Result = response.Result
	responseExecutionReport.Reports = make(map[ExecutionReportId]*function.ExecutionReport)
	for key, report := range response.Reports {
		responseExecutionReport.Reports[ExecutionReportId(key)] = report
	}

	responseExecutionReport.ResponseTime = now.Sub(r.Arrival).Seconds()

	tReturnDuration := receivedTime - response.Treturn
	tTransferDuration := response.Ttransfer
	fmt.Println("TtransferDuration: ", tTransferDuration)
	fmt.Println("TreturnDuration: ", tReturnDuration)
	fmt.Println("RemoteExecutionDuration: ", response.ResponseTime)

	responseExecutionReport.Ttransfer = tTransferDuration
	responseExecutionReport.Treturn = tReturnDuration
	responseExecutionReport.AvailableRemoteMemMB = response.AvailableRemoteMemMB

	fmt.Println("AvailableRemoteMemMB: ", responseExecutionReport.AvailableRemoteMemMB)

	if telemetry.DefaultTracer != nil {
		trace.SpanFromContext(r.Ctx).AddEvent("Process Offload Post response complete")
	}

	// TODO: check how this is used in the QoSAware policy
	// It was originially computed as "report.Arrival - sendingTime"
	//r.ExecReport.OffloadLatency = now.Sub(sendingTime).Seconds() - r.ExecReport.Duration - r.ExecReport.InitTime
	//r.ExecReport.SchedAction = SCHED_ACTION_OFFLOAD
	return responseExecutionReport, false, nil
}
