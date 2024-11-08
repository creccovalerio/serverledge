package fc

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/cornelk/hashmap"
	"github.com/grussorusso/serverledge/internal/client"
	"github.com/grussorusso/serverledge/internal/function"
	"github.com/grussorusso/serverledge/internal/node"
)

const SCHED_ACTION_OFFLOAD = "O"

// TODO: offload the entire node when is cloud only
func WorkflowOffload(r *CompositionRequest, serverUrl string, reports *hashmap.Map[ExecutionReportId, *function.ExecutionReport]) (CompositionExecutionReport, error) {

	exe_reports := make(map[string]*function.ExecutionReport) // make(map[fc.ExecutionReportId]*function.ExecutionReport)

	reports.Range(func(id ExecutionReportId, report *function.ExecutionReport) bool {
		fmt.Println("-----> REMOTE REPORT ID: ", string(id), report)
		exe_reports[string(id)] = report
		return true
	})

	request := client.CompositionInvocationRequest{
		ReqId:   r.ReqId,
		Params:  r.Params,
		Reports: exe_reports,
		//QoSClass: api.DecodeServiceClass(qosClass),
		CanDoOffloading: false, // blocking another possible offload of the same request
		//Async:           r.Async
	}
	invocationBody, err := json.Marshal(request)
	if err != nil {
		log.Print(invocationBody)
		return CompositionExecutionReport{}, err
	}

	fmt.Println("\nSENDING POST: ", serverUrl, r.Fc.Name)
	resp, err := offloadingClient.Post(serverUrl+"/offload/"+r.Fc.Name, "application/json",
		bytes.NewBuffer(invocationBody))

	//url := fmt.Sprintf("%s/offload/%s", serverUrl, r.Fc.Name)
	//resp, err := utils.PostJson(url, invocationBody)
	if err != nil {
		fmt.Println("\nSENDING FAIL")
		log.Print(err)
		return CompositionExecutionReport{}, err
	}
	fmt.Println("\nSENDING Ok:", resp.StatusCode)

	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusTooManyRequests {
			return CompositionExecutionReport{}, node.OutOfResourcesErr
		}
		return CompositionExecutionReport{}, fmt.Errorf("Remote returned: %v", resp.StatusCode)
	}

	//var response function.Response
	var responseExecutionReport CompositionExecutionReport
	var response CompositionResponse
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			fmt.Printf("Error while closing offload response body: %s\n", err)
		}
	}(resp.Body)
	body, _ := io.ReadAll(resp.Body)
	if err = json.Unmarshal(body, &response); err != nil {
		return CompositionExecutionReport{}, err
	}

	now := time.Now()
	response.ResponseTime = now.Sub(r.Arrival).Seconds()
	fmt.Println("RESULT: ", response.Result, &response.Reports)
	responseExecutionReport.Result = response.Result
	responseExecutionReport.Reports = hashmap.New[ExecutionReportId, *function.ExecutionReport]()
	for key, value := range response.Reports {
		responseExecutionReport.Reports.Set(ExecutionReportId(key), value)
	}

	// TODO: check how this is used in the QoSAware policy
	// It was originially computed as "report.Arrival - sendingTime"
	//r.ExecReport.OffloadLatency = now.Sub(sendingTime).Seconds() - r.ExecReport.Duration - r.ExecReport.InitTime
	//r.ExecReport.SchedAction = SCHED_ACTION_OFFLOAD
	return responseExecutionReport, nil
}
