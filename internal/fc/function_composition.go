package fc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"runtime"
	"time"

	"github.com/grussorusso/serverledge/internal/cache"
	"github.com/grussorusso/serverledge/internal/config"
	"github.com/grussorusso/serverledge/internal/function"
	"github.com/grussorusso/serverledge/internal/node"
	"github.com/grussorusso/serverledge/internal/telemetry"
	"github.com/grussorusso/serverledge/internal/types"
	"github.com/grussorusso/serverledge/utils"
	"github.com/labstack/gommon/log"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/exp/slices"
)

var offloadingClient *http.Client
var requests chan *scheduledFcRequest
var completions chan *completion
var remoteServerUrl string
var dataMap map[time.Time]ReturnedQueryMetrics
var reqIds []ReqId // slice of ReqId to delete pd & progress periodically

// FunctionComposition is a serverless Function Composition
type FunctionComposition struct {
	Name               string // al posto del nome potrebbe essere un id da mettere in etcd
	Functions          map[string]*function.Function
	Workflow           Dag
	RemoveFnOnDeletion bool
}

type scheduledFcRequest struct {
	*CompositionRequest
	progress          *Progress
	fcDecisionChannel chan fcSchedDecision
}

type completion struct {
	*scheduledFcRequest
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

type ReturnedQueryMetrics struct {
	AvgTotalColdStartsTime   map[string]float64
	AvgFunDurationTime       map[string]float64
	AvgFunRemoteDurationTime map[string]float64
	AvgOutputFunSize         map[string]float64
	AvgOutputFunRemoteSize   map[string]float64
	AvgFcRespTime            map[string]float64
	AvgFcRemoteRespTime      map[string]float64
	AvgSavingInfosTime       map[string]float64
	AvgFcTTransferTime       map[string]float64
	AvgFcTReturnTime         map[string]float64
	NoChoiceNodeInvocations  map[string]float64
	NoBranchInvocations      map[string]float64
}

type ExecutionReportId string

func CreateExecutionReportId(dagNode DagNode) ExecutionReportId {
	return ExecutionReportId(printType(dagNode.GetNodeType()) + "_" + string(dagNode.GetId()))
}

type CompositionExecutionReport struct {
	Result               map[string]interface{}
	Reports              map[ExecutionReportId]*function.ExecutionReport
	ResponseTime         float64 // time waited by the user to get the output of the entire composition
	Ttransfer            float64 // duration of request transfer in remote
	Treturn              float64 // duration of response transfer
	AvailableRemoteMemMB int64
	Progress             *Progress `json:"-"` // skipped in Json marshaling
}

func (cer *CompositionExecutionReport) GetSingleResult() (string, error) {
	if len(cer.Result) == 1 {
		for _, value := range cer.Result {
			return fmt.Sprintf("%v", value), nil
		}
	}
	return "", fmt.Errorf("there is not exactly one result: there are %d result(s)", len(cer.Result))
}

func (cer *CompositionExecutionReport) GetIntSingleResult() (int, error) {
	if len(cer.Result) == 1 {
		for _, value := range cer.Result {
			valueInt, ok := value.(int)
			if !ok {
				return 0, fmt.Errorf("value %v cannot be casted to int", value)
			}
			return valueInt, nil
		}
	}
	return 0, fmt.Errorf("there is not exactly one result: there are %d result(s)", len(cer.Result))
}

func (cer *CompositionExecutionReport) GetAllResults() string {
	result := "[\n"
	for _, value := range cer.Result {
		result += fmt.Sprintf("\t%v\n", value)
	}
	result += "]\n"
	return result
}

// NewFC instantiates a new FunctionComposition with a name and a corresponding dag. The functions parameter can contain duplicate functions (with the same name)
func NewFC(name string, dag Dag, functions []*function.Function, removeFnOnDeletion bool) (*FunctionComposition, error) {
	functionMap := make(map[string]*function.Function)
	if functions != nil {
		for _, f := range functions {
			// if the function is already added, simply replace it
			functionMap[f.Name] = f
		}
	}

	// if not all unique functions are present inside the functions array, we return an error
	definedFunctions := dag.GetUniqueDagFunctions()
	for _, f := range definedFunctions {
		_, ok2 := functionMap[f]
		if !ok2 {
			return nil, fmt.Errorf("the function %s is not included in the FunctionComposition functions parameter, but it must be registered to Serverledge", f)
		}
	}

	return &FunctionComposition{
		Name:               name,
		Functions:          functionMap,
		Workflow:           dag,
		RemoveFnOnDeletion: removeFnOnDeletion,
	}, nil
}

func (fc *FunctionComposition) getEtcdKey() string {
	return getEtcdKey(fc.Name)
}

func getEtcdKey(fc string) string {
	return fmt.Sprintf("/fc/%s", fc)
}

// GetAllFC returns the function composition names
func GetAllFC() ([]string, error) {
	return function.GetAllWithPrefix("/fc")
}

func getFCFromCache(name string) (*FunctionComposition, bool) {
	localCache := cache.GetCacheInstance()
	cachedObj, found := localCache.Get(name)
	if !found {
		return nil, false
	}
	//cache hit
	//return a safe copy of the function composition previously obtained
	fc := *cachedObj.(*FunctionComposition)
	return &fc, true
}

func getFCFromEtcd(name string) (*FunctionComposition, error) {
	cli, err := utils.GetEtcdClient()
	if err != nil {
		return nil, errors.New("failed to connect to ETCD")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	key := getEtcdKey(name)
	getResponse, err := cli.Get(ctx, key)
	if err != nil || len(getResponse.Kvs) < 1 {
		return nil, fmt.Errorf("failed to retrieve value for key %s", key)
	}

	var f FunctionComposition
	err = json.Unmarshal(getResponse.Kvs[0].Value, &f)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal json: %v", err)
	}

	return &f, nil
}

// GetFC gets the FunctionComposition from cache or from ETCD
func GetFC(name string) (*FunctionComposition, bool) {
	val, found := getFCFromCache(name)
	if !found {
		// cache miss
		f, err := getFCFromEtcd(name)
		if err != nil {
			return nil, false
		}
		//insert a new element to the cache
		cache.GetCacheInstance().Set(name, f, cache.DefaultExp)
		return f, true
	}

	return val, true
}

// SaveToEtcd creates and register the function composition in Serverledge
// It is like SaveToEtcd for a simple function
func (fc *FunctionComposition) SaveToEtcd() error {
	cli, err := utils.GetEtcdClient()
	if err != nil {
		return err
	}
	ctx := context.TODO()

	// Save all functions in the dag to ETCD
	// funcs := make([]*function.Function, 0)
	for _, fName := range fc.Workflow.GetUniqueDagFunctions() {
		_, exists := function.GetFunction(fName)
		if !exists {
			errSave := fc.Functions[fName].SaveToEtcd()
			if errSave != nil {
				return fmt.Errorf("failed to save function %s: %v", fName, errSave)
			}
		}
		// funcs = append(funcs, f)
	}

	// marshal the function composition object into json
	payload, err := json.Marshal(*fc)
	if err != nil {
		return fmt.Errorf("could not marshal function composition: %v", err)
	}
	// saves the json object into etcd
	_, err = cli.Put(ctx, fc.getEtcdKey(), string(payload))
	if err != nil {
		return fmt.Errorf("failed etcd Put: %v", err)
	}

	// Add the function composition to the local cache
	cache.GetCacheInstance().Set(fc.Name, fc, cache.DefaultExp)

	return nil
}

func DeletePdAndProgressFromEtcd() {
	var err error
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		if len(reqIds) != 0 {
			for _, requestId := range reqIds {
				fmt.Println("There are pd & progress to delete for requestId: ", requestId)
				err = DeleteProgressFromEtcd(requestId)
				if err != nil {
					panic(err)
				}
				_, err := DeleteAllPartialDataFromEtcd(requestId)
				if err != nil {
					panic(err)
				}
				reqIds = reqIds[1:] // remove the processed element (requestId) from reqIds
			}
		}
	}
}

func Run(p FcPolicy) {
	requests = make(chan *scheduledFcRequest, 500)
	completions = make(chan *completion, 500)
	dataMap = make(map[time.Time]ReturnedQueryMetrics)

	// initialize Resources resources
	availableCores := runtime.NumCPU()
	node.Resources.AvailableMemMB = int64(config.GetInt(config.POOL_MEMORY_MB, 1024))
	node.Resources.AvailableCPUs = config.GetFloat(config.POOL_CPUS, float64(availableCores))
	log.Printf("Current resources: %v\n", &node.Resources)

	tr := &http.Transport{
		MaxIdleConns:        2500,
		MaxIdleConnsPerHost: 2500,
		MaxConnsPerHost:     0,
		IdleConnTimeout:     30 * time.Minute,
	}
	offloadingClient = &http.Client{Transport: tr}

	// initialize workflow scheduling policy
	p.Init()

	remoteServerUrl = config.GetString(config.CLOUD_URL, "")

	fmt.Println("Fc Scheduler started, with CLOUD_URL: ", remoteServerUrl)

	var r *scheduledFcRequest
	var c *completion
	for {
		select {
		case r = <-requests: // receive request
			go p.OnArrival(r)
		case c = <-completions:
			p.OnCompletion(c.scheduledFcRequest)
		}
	}

}

func dropRequest(r *scheduledFcRequest) {
	r.fcDecisionChannel <- fcSchedDecision{action: DROP}
}

func handleOffload(r *scheduledFcRequest, serverHost string) {
	r.CanDoOffloading = false // the next server can't offload this request
	r.fcDecisionChannel <- fcSchedDecision{
		action:     EXEC_REMOTE,
		remoteHost: serverHost,
	}
}

func handleCloudOffload(r *scheduledFcRequest) {
	cloudAddress := config.GetString(config.CLOUD_URL, "")
	handleOffload(r, cloudAddress)
}

func handleLocal(r *scheduledFcRequest) {
	r.fcDecisionChannel <- fcSchedDecision{
		action: EXEC_LOCAL,
	}
}

func handleExecuteLocal(r *scheduledFcRequest) {
	handleLocal(r)
}

// Save PartialData & Progress on etcd with a single access
func saveDataToEtcd(pd *PartialData, p *Progress) error {
	// save in ETCD
	cli, err := utils.GetEtcdClient()
	if err != nil {
		return err
	}
	ctx := context.TODO()

	// marshal the partialdatas object into json
	payloadPartialData, err := json.Marshal(pd)
	if err != nil {
		return fmt.Errorf("could not marshal partialData: %v", err)
	}
	// marshal the progress object into json
	payloadProgress, err := json.Marshal(p)
	if err != nil {
		return fmt.Errorf("could not marshal progress: %v", err)
	}
	// saves the json object into etcd
	keyPartialData := getPartialDataEtcdKey(pd.ReqId, pd.ForNode)
	pdEtcdMutex.Lock()
	defer pdEtcdMutex.Unlock()
	_, err = cli.Put(ctx, keyPartialData, string(payloadPartialData))
	if err != nil {
		return fmt.Errorf("failed etcd Put partial data: %v", err)
	}
	keyProgress := getProgressEtcdKey(p.ReqId)
	progressMutexEtcd.Lock()
	defer progressMutexEtcd.Unlock()
	_, err = cli.Put(ctx, keyProgress, string(payloadProgress))
	if err != nil {
		return fmt.Errorf("failed etcd Put: %v", err)
	}
	return nil
}

// Invoke schedules each function of the composition and invokes them
func (fc *FunctionComposition) Invoke(r *CompositionRequest) (CompositionExecutionReport, error) {

	var err error
	var offloadOccurred = false
	var response CompositionExecutionReport
	requestId := ReqId(r.Id())
	input := r.Params

	// initialize struct progress from dag
	progress := InitProgressRecursive(requestId, &fc.Workflow)

	// initialize partial data with input, directly from the Start.Next node
	pd := NewPartialData(requestId, fc.Workflow.Start.Next, fc.Workflow.Start.Id, input)
	pd.Data = input

	schedFcRequest := scheduledFcRequest{
		CompositionRequest: r,
		progress:           progress,
		fcDecisionChannel:  make(chan fcSchedDecision, 1)}

	shouldContinue := true
	for shouldContinue {
		requests <- &schedFcRequest // send request

		if telemetry.DefaultTracer != nil {
			trace.SpanFromContext(r.Ctx).AddEvent("Fc Scheduling start")
		}

		fcSchedDecision, ok := <-schedFcRequest.fcDecisionChannel
		if !ok {
			return CompositionExecutionReport{Result: nil, Progress: progress}, fmt.Errorf("failed scheduling fc request execution: %v", err)
		}

		if telemetry.DefaultTracer != nil {
			trace.SpanFromContext(r.Ctx).AddEvent("Fc Scheduling complete")
		}

		if fcSchedDecision.action == EXEC_LOCAL {
			pd, progress, shouldContinue, err = fc.Workflow.Execute(r, pd, progress)
			if err != nil {
				progress.Print()
				return CompositionExecutionReport{Result: nil, Progress: progress}, fmt.Errorf("failed dag execution: %v", err)
			}
			schedFcRequest.CompositionRequest.Iteration++
			schedFcRequest.progress = progress
			r.ExecReport.ResponseTime = time.Since(r.Arrival).Seconds()
			r.ExecReport.Ttransfer = 0
			r.ExecReport.Treturn = 0
		} else if fcSchedDecision.action == EXEC_REMOTE {
			if telemetry.DefaultTracer != nil {
				trace.SpanFromContext(r.Ctx).AddEvent("Save pd & progress on etcd start")
			}

			startSaving := time.Now()

			saveDataToEtcd(pd, progress)

			endSaving := time.Since(startSaving).Seconds()
			fmt.Println("SAVING DURATION: ", endSaving)

			utils.CreateAndRecordNewHistogramMetric("FunctionComposition.SavingInfosDuration", "Save Pd & Progress on etcd duration", r.Ctx, endSaving, "functionCompositionSavingInfoDuration", r.Fc.Name)

			if telemetry.DefaultTracer != nil {
				trace.SpanFromContext(r.Ctx).AddEvent("Save pd & progress on etcd complete")
			}

			/* flag to use in order to execute DeleteProgress and DeleteAllPartialData only if
			 * progress and partial data have been stored in etcd during workflow offloading */
			offloadOccurred = true

			// preparing workflow offloading request
			response, shouldContinue, err = WorkflowOffload(r, fcSchedDecision.remoteHost, r.ExecReport.Reports)
			if err != nil {
				return CompositionExecutionReport{}, err
			}

			pd.Data = response.Result // WorkflowOffload has executed interaly the remaining part of the workflow
			r.ExecReport.Reports = response.Reports
			r.ExecReport.ResponseTime = time.Since(r.Arrival).Seconds()
			r.ExecReport.Ttransfer = response.Ttransfer
			r.ExecReport.Treturn = response.Treturn
			r.ExecReport.AvailableRemoteMemMB = response.AvailableRemoteMemMB
			schedFcRequest.CompositionRequest.Iteration++
			schedFcRequest.progress = progress
			/* metricFcRemoteRespTime is the response time without the initTime
			* of the containers */
			metricFcRemoteRespTime := r.ExecReport.ResponseTime
			for _, funcReport := range r.ExecReport.Reports {
				metricFcRemoteRespTime -= funcReport.InitTime
				if funcReport.FunctionName != "" {
					utils.CreateAndRecordNewHistogramMetric("Function.RemoteDuration", "Duration of a function executed remotly", r.Ctx, funcReport.Duration, "functInvocationRemoteDuration", funcReport.FunctionName)
					utils.CreateAndRecordNewHistogramMetric("FunctionOutput.RemoteSize", "Size of the function output executed remotly", r.Ctx, float64(len([]byte(funcReport.Result))), "functionRemoteSizeHistogram", funcReport.FunctionName)
				}
			}
			utils.CreateAndRecordNewHistogramMetric("FunctionComposition.remoteRespTime", "Remote response time of a function composition", r.Ctx, metricFcRemoteRespTime, "functionCompositionInvocationRemoteRespTime", r.Fc.Name)
			attributeValue := fmt.Sprintf("%s_%s", r.Fc.Name, utils.FormatParams(r.Params))
			utils.CreateAndRecordNewHistogramMetric("FunctionComposition.remoteRespTimePerInput", "Remote response time of a function composition Per Input", r.Ctx, metricFcRemoteRespTime, "functionCompositionInvocationRemoteRespTimePerInput", attributeValue)
			utils.CreateAndRecordNewHistogramMetric("FunctionComposition.TTransferTime", "Duration for sending the remote execution request", r.Ctx, r.ExecReport.Ttransfer, "functionCompositionTTransferTime", r.Fc.Name)
			utils.CreateAndRecordNewHistogramMetric("FunctionComposition.TReturnTime", "Duration for receiving the remote execution response", r.Ctx, r.ExecReport.Treturn, "functionCompositionTReturnTime", r.Fc.Name)
		} else {
			// drop case
			return CompositionExecutionReport{}, node.OutOfResourcesErr
		}

	}

	if offloadOccurred {
		/* saving requestIds of offloaded requests in order to pass them
		 * to the goroutine to do the periodic delete of pd & progress
		 * associated with a specific requestId */
		reqIds = append(reqIds, requestId)
	} else {
		if telemetry.MetricsEnabled {
			/* metricFcRespTime is the response time without the initTime
			 * of the containers */
			metricFcRespTime := r.ExecReport.ResponseTime
			for _, funcReport := range r.ExecReport.Reports {
				if funcReport.InitTime != 0 {
					metricFcRespTime -= funcReport.InitTime
				}
			}
			utils.CreateAndRecordNewHistogramMetric("FunctionComposition.respTime", "Response time of a function composition", r.Ctx, metricFcRespTime, "functionCompositionInvocationRespTime", r.Fc.Name)
			attributeValue := fmt.Sprintf("%s_%s", r.Fc.Name, utils.FormatParams(r.Params))
			utils.CreateAndRecordNewHistogramMetric("FunctionComposition.respTimePerInput", "Response time of a function composition per input", r.Ctx, metricFcRespTime, "functionCompositionInvocationRespTimePerInput", attributeValue)
		}
	}

	r.ExecReport.Result = pd.Data

	return r.ExecReport, nil
}

// Invoke schedules each function of the composition and invokes them
func (fc *FunctionComposition) InvokeFunctionCompositionOffload(r *CompositionRequest) (CompositionExecutionReport, error) {

	var err error
	var pd *PartialData
	requestId := ReqId(r.Id())
	// retrieve struct progress from dag
	if telemetry.DefaultTracer != nil {
		trace.SpanFromContext(r.Ctx).AddEvent("Retrieve pd & progress from etcd start")
	}
	progress, found := RetrieveProgressFromEtcd(requestId)
	if !found {
		return CompositionExecutionReport{Result: nil, Progress: progress}, fmt.Errorf("progress not found")
	}

	// retriving partial data
	nextNodes, err := progress.NextNodes()
	if err != nil {
		return CompositionExecutionReport{Result: nil, Progress: progress}, fmt.Errorf("failed to get next nodes from progress: %v", err)
	}

	n, ok := r.Fc.Workflow.Find(nextNodes[0])
	if !ok {
		return CompositionExecutionReport{Result: nil, Progress: progress}, fmt.Errorf("failed to get next nodes from progress: %v", err)
	}

	switch n.(type) {
	case *StartNode:
		pd = NewPartialData(requestId, fc.Workflow.Start.Next, fc.Workflow.Start.Id, r.Params)
	default:
		pd, err = RetrieveSinglePartialDataFromEtcd(requestId, nextNodes[0])
		if err != nil {
			return CompositionExecutionReport{Result: nil, Progress: progress}, fmt.Errorf("failed to get partial data: %v", err)
		}
	}

	if telemetry.DefaultTracer != nil {
		trace.SpanFromContext(r.Ctx).AddEvent("Retrieve pd & progress from etcd complete")
	}

	shouldContinue := true
	for shouldContinue {
		// executing dag
		pd, progress, shouldContinue, err = fc.Workflow.Execute(r, pd, progress)
		if err != nil {
			progress.Print()
			return CompositionExecutionReport{Result: nil, Progress: progress}, fmt.Errorf("failed dag execution: %v", err)
		}
		r.ExecReport.ResponseTime = time.Since(r.Arrival).Seconds()
	}

	r.ExecReport.Result = pd.Data
	return r.ExecReport, nil
}

// Delete removes the FunctionComposition from cache and from etcd, so it cannot be invoked anymore
func (fc *FunctionComposition) Delete() error {
	cli, err := utils.GetEtcdClient()
	if err != nil {
		return err
	}
	ctx := context.TODO()
	if fc.RemoveFnOnDeletion {
		for _, f := range fc.Functions {
			err = f.Delete()
			if err != nil {
				fmt.Printf("failed to delete function %s associated to function composition %s: %v", f.Name, fc.Name, err)
			}
		}
	}

	dresp, err := cli.Delete(ctx, fc.getEtcdKey())
	if err != nil || dresp.Deleted != 1 {
		return fmt.Errorf("failed Delete: %v", err)
	}

	// Remove the function from the local cache
	cache.GetCacheInstance().Delete(fc.Name)

	return nil
}

// DeleteAll deletes the function composition from Etcd and the Functions associated with it
func (fc *FunctionComposition) DeleteAll() error {
	err := fc.Delete()

	for _, fName := range fc.Workflow.GetUniqueDagFunctions() {
		f, exists := function.GetFunction(fName)
		if !exists {
			return fmt.Errorf("funtion %s does not exist", fName)
		}
		err1 := f.Delete()
		if err1 != nil {
			return fmt.Errorf("the deletion of the function %s has failed", f.Name)
		}
	}

	return err
}

// Exists return true if the function composition exists either in etcd or in cache. If it only exists in Etcd, it saves the composition also in caches
func (fc *FunctionComposition) Exists() bool {
	_, found := getFCFromCache(fc.Name)
	if !found {
		// cache miss
		f, err := getFCFromEtcd(fc.Name)
		if err != nil {
			if err.Error() == fmt.Sprintf("failed to retrieve value for key %s", getEtcdKey(fc.Name)) {
				return false
			} else {
				log.Error(err.Error())
				return false
			}
		}
		//insert a new element to the cache
		cache.GetCacheInstance().Set(f.Name, f, cache.DefaultExp)
		return true
	}
	return found
}

// Equals is used in tests to check function composition equality
func (fc *FunctionComposition) Equals(cmp types.Comparable) bool {
	fc2 := cmp.(*FunctionComposition)
	if fc.Name != fc2.Name {
		return false
	}
	funcs1 := fc.Workflow.GetUniqueDagFunctions()
	funcs2 := fc2.Workflow.GetUniqueDagFunctions()
	if !slices.Equal(funcs1, funcs2) {
		return false
	}
	if !fc.Workflow.Equals(&fc2.Workflow) {
		return false
	}
	return true
}

func (fc *FunctionComposition) String() string {
	functions := "["
	i := 0
	for name, _ := range fc.Functions {
		functions += name
		if i < len(fc.Functions)-1 {
			functions += ", "
		}
		i++
	}
	functions += "]"
	workflow := fc.Workflow.String()
	return fmt.Sprintf(`FunctionComposition{
		Name: %s,
		Functions: %s,
		Workflow:\n%s,
		RemoveFnOnDeletion: %t
	}`, fc.Name, functions, workflow, fc.RemoveFnOnDeletion)
}

// MarshalJSON for CompositionExecutionReport is necessary as the hashmap cannot be directly marshaled
func (cer CompositionExecutionReport) MarshalJSON() ([]byte, error) {
	// Create a map to hold the JSON representation of the FunctionComposition
	data := make(map[string]interface{})
	data["Result"] = cer.Result // al posto del nome potrebbe essere un id da mettere in etcd
	data["ResponseTime"] = cer.ResponseTime

	//reports := make(map[ExecutionReportId]*function.ExecutionReport)

	//cer.Reports.Range(func(id ExecutionReportId, report *function.ExecutionReport) bool {
	//	reports[id] = report
	//	return true
	//})
	data["Reports"] = cer.Reports

	return json.Marshal(data)
}

func (cer CompositionExecutionReport) UnmarshalJSON(data []byte) error {
	var tempMap map[string]json.RawMessage
	if err := json.Unmarshal(data, &tempMap); err != nil {
		return err
	}

	if rawResult, ok := tempMap["Result"]; ok {
		if err := json.Unmarshal(rawResult, &cer.Result); err != nil {
			return err
		}
	} else {
		return fmt.Errorf("missing 'Result' field in JSON")
	}

	if rawResponseTime, ok := tempMap["ResponseTime"]; ok {
		if err := json.Unmarshal(rawResponseTime, &cer.ResponseTime); err != nil {
			return err
		}
	} else {
		return fmt.Errorf("missing 'ResponseTime' field in JSON")
	}

	//if rawProgress, ok := tempMap["Progress"]; ok {
	//	if err := json.Unmarshal(rawProgress, &cer.Progress); err != nil {
	//		return err
	//	}
	//} else {
	//	return fmt.Errorf("missing 'Progress' field in JSON")
	//}
	var tempReportsMap map[string]json.RawMessage
	if err := json.Unmarshal(tempMap["Reports"], &tempReportsMap); err != nil {
		return err
	}
	for id, execReport := range tempReportsMap {
		var execReportVar function.ExecutionReport
		err := json.Unmarshal(execReport, &execReportVar)
		if err != nil {
			return err
		}
		cer.Reports[ExecutionReportId(id)] = &execReportVar
	}
	return nil
}

func (cer *CompositionExecutionReport) String() string {
	str := "["
	str += fmt.Sprintf("\n\tResponseTime: %f,", cer.ResponseTime)
	str += "\n\tReports: ["
	if len(cer.Reports) > 0 {
		j := 0
		for id, report := range cer.Reports {
			schedAction := "''"
			if report.SchedAction != "" {
				schedAction = report.SchedAction
			}
			output := "''"
			if report.Output != "" {
				output = report.Output
			}

			str += fmt.Sprintf("\n\t\t%s: {FunctionName: %s, ResponseTime: %f, IsWarmStart: %v, InitTime: %f, ColdStartTime: %f, OffloadLatency: %f, Duration: %f, SchedAction: %v, Output: %s, Result: %s}", id, report.FunctionName, report.ResponseTime, report.IsWarmStart, report.InitTime, report.ColdStartTime, report.OffloadLatency, report.Duration, schedAction, output, report.Result)
			if j < len(cer.Reports)-1 {
				str += ","
			}
			if j == len(cer.Reports)-1 {
				str += "\n\t]"
			}
			j++
		}
	}

	str += "\n\tResult: {"
	i := 0
	lll := len(cer.Result)
	for s, v := range cer.Result {
		if i == 0 {
			str += "\n"
		}
		str += fmt.Sprintf("\t\t%s: %v,", s, v)
		if i < lll-1 {
			str += ",\n"
		} else if i == lll-1 {
			str += "\n"
		}
		i++
	}
	str += "\t}\n}\n"
	return str
}

func (cer *CompositionExecutionReport) Equals(other types.Comparable) bool {
	cer2, ok := other.(*CompositionExecutionReport)
	if !ok {
		fmt.Printf("other type %T is not CompositionExecutionReport\n", other)
		return false
	}

	allEquals := true
	//cer.Reports.Range(func(id ExecutionReportId, report *function.ExecutionReport) bool {
	for id, report := range cer.Reports {
		//report2, isPresent := cer2.Reports.Get(id)
		report2, isPresent := cer2.Reports[id]
		if !isPresent {
			fmt.Printf("element %s is not present in the other report", id)
			allEquals = false
			return false
		}
		fieldAllEqual := true
		if report.Output != report2.Output {
			fmt.Printf("Output: report1 '%v' is different from report2 '%v'\n", report.Output, report2.Output)
			fieldAllEqual = false
		}

		if report.Duration != report2.Duration {
			fmt.Printf("Duration: report1 '%v' is different from report2 '%v'\n", report.Duration, report2.Duration)
			fieldAllEqual = false
		}

		if report.Result != report2.Result {
			fmt.Printf("Result: report1 '%v' is different from report2 '%v'\n", report.Result, report2.Result)
			fieldAllEqual = false
		}

		if report.OffloadLatency != report2.OffloadLatency {
			fmt.Printf("OffloadLatency: report1 '%v' is different from report2 '%v'\n", report.OffloadLatency, report2.OffloadLatency)
			fieldAllEqual = false
		}

		if report.ResponseTime != report2.ResponseTime {
			fmt.Printf("ResponseTime: report1 '%v' is different from report2 '%v'\n", report.ResponseTime, report2.ResponseTime)
			fieldAllEqual = false
		}

		if report.SchedAction != report2.SchedAction {
			fmt.Printf("SchedAction: report1 '%v' is different from report2 '%v'\n", report.SchedAction, report2.SchedAction)
			fieldAllEqual = false
		}

		if report.FunctionName != report2.FunctionName {
			fmt.Printf("FunctionName: report1 '%v' is different from report2 '%v'\n", report.FunctionName, report2.FunctionName)
			fieldAllEqual = false
		}

		if report.InitTime != report2.InitTime {
			fmt.Printf("InitTime: report1 '%v' is different from report2 '%v'\n", report.InitTime, report2.InitTime)
			fieldAllEqual = false
		}

		if report.ColdStartTime != report2.ColdStartTime {
			fmt.Printf("ColdStart: report1 '%v' is different from report2 '%v'\n", report.ColdStartTime, report2.ColdStartTime)
			fieldAllEqual = false
		}

		if report.IsWarmStart != report2.IsWarmStart {
			fmt.Printf("IsWarmStart: report1 '%v' is different from report2 '%v'\n", report.IsWarmStart, report2.IsWarmStart)
			fieldAllEqual = false
		}

		if !fieldAllEqual {
			allEquals = false
			return false
		}
		//return true
	}

	if cer.ResponseTime != cer2.ResponseTime {
		fmt.Printf("Composition ResponseTime: %f is different from %f", cer.ResponseTime, cer2.ResponseTime)
		return false
	}

	for key, value := range cer.Result {
		value2, exists := cer2.Result[key]
		if !exists {
			fmt.Printf("Composition Result: key '%s' is not present in the other composition report\n", key)
			return false
		}
		if value != value2 {
			fmt.Printf("Composition Result: value for key '%s' is different from the other composition report. First is %v of type %T, second is %v of type %T\n", key, value, value, value2, value2)
			return false
		}
	}

	return allEquals
}
