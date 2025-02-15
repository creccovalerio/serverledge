package metrics

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/grussorusso/serverledge/internal/config"
	"github.com/grussorusso/serverledge/internal/fc"
	"github.com/grussorusso/serverledge/internal/scheduling"
	go_api "github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/model"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

var dataToSend fc.ReturnedQueryMetrics
var dataFuncToSend scheduling.ReturnedFunctionQueryMetrics

// Struct to represent query with its id
type queryInfos struct {
	id    string
	query string
}

func queryPrometheus(wg *sync.WaitGroup, queryInfos queryInfos, api v1.API, ctx context.Context, byTerm string) {
	defer wg.Done()

	var outputMap map[string]float64
	result, warnings, err := api.Query(ctx, queryInfos.query, time.Now())
	if err != nil {
		log.Fatalf("Error with the execution of the query: %v\n", err)
	}

	if len(warnings) > 0 {
		log.Printf("Received Advices in the execution of the query: %v\n", warnings)
	}

	outputMap = parserOutputResult(result, byTerm)

	switch queryInfos.id {
	case "AvgTotalColdStartsTime":
		dataToSend.AvgTotalColdStartsTime = outputMap
		dataFuncToSend.AvgTotalColdStartsTime = outputMap
	case "AvgFunDurationTime":
		dataToSend.AvgFunDurationTime = outputMap
		dataFuncToSend.AvgFunDurationTime = outputMap
	case "AvgOutputFunSize":
		dataToSend.AvgOutputFunSize = outputMap
		dataFuncToSend.AvgOutputFunSize = outputMap
	case "AvgRemoteFunDurationTime":
		dataToSend.AvgFunRemoteDurationTime = outputMap
		dataFuncToSend.AvgFunRemoteDurationTime = outputMap
	case "AvgRemoteOutputFunSize":
		dataToSend.AvgOutputFunRemoteSize = outputMap
		dataFuncToSend.AvgOutputFunRemoteSize = outputMap
	case "AvgFcRespTime":
		dataToSend.AvgFcRespTime = outputMap
	case "AvgFcRemoteRespTime":
		dataToSend.AvgFcRemoteRespTime = outputMap
	case "AvgSavingInfosTime":
		dataToSend.AvgSavingInfosTime = outputMap
	case "AvgFcTTranferTime":
		dataToSend.AvgFcTTransferTime = outputMap
	case "AvgFcTReturnTime":
		dataToSend.AvgFcTReturnTime = outputMap
	case "NoChoiceNodeInvocations":
		dataToSend.NoChoiceNodeInvocations = outputMap
	case "NoBranchInvocations":
		dataToSend.NoBranchInvocations = outputMap
	}

}

func parserOutputResult(execResult model.Value, byTerm string) map[string]float64 {
	functionValues := make(map[string]float64)
	if vector, ok := execResult.(model.Vector); ok {
		for _, sample := range vector {
			if byTerm == "" {
				value := float64(sample.Value)
				functionValues["result"] = value
			} else {
				functionName := string(sample.Metric[model.LabelName(byTerm)])
				value := float64(sample.Value)
				functionValues[functionName] = value
			}

		}
	} else {
		log.Fatalf("Unexpected Result %v\n", execResult)
	}

	return functionValues
}

func parserBy(query string) string {
	re := regexp.MustCompile(`by\s*\(([^)]+)\)`)
	var concatenated string
	matches := re.FindAllStringSubmatch(query, -1)
	if matches != nil {
		uniqueTerms := make(map[string]bool)

		for _, match := range matches {
			if len(match) > 1 {
				terms := strings.Split(match[1], ",")
				for _, term := range terms {
					trimmedTerm := strings.TrimSpace(term)
					uniqueTerms[trimmedTerm] = true
				}
			}
		}

		var byContents []string
		for term := range uniqueTerms {
			byContents = append(byContents, term)
		}

		concatenated = strings.Join(byContents, ", ")
	} else {
		//byTerm not found
		concatenated = ""
	}

	return concatenated
}

func PeriodicalMetricsRetrieveFromPrometheus() {
	// Configuration of the Prometheus client
	client, err := go_api.NewClient(go_api.Config{
		Address: "http://127.0.0.1:9090",
	})
	if err != nil {
		log.Fatalf("Error in client creation: %v\n", err)
	}

	// API of Prometheus
	api := v1.NewAPI(client)
	ctx := context.Background()

	queries := []queryInfos{
		//{"sum(rate(ColdStart_duration_seconds_sum[1m])) / clamp_min(sum(rate(ColdStart_duration_seconds_count[1m])),1)", "AVG Cold Start Duration [1m]"},
		{"AvgTotalColdStartsTime", "sum(ColdStart_duration_seconds_sum) by (functColdStartHistogram) / clamp_min(sum(ColdStart_duration_seconds_count) by (functColdStartHistogram), 1)"},
		{"AvgFunDurationTime", "sum(Function_duration_seconds_sum) by (functInvocationDuration) / sum(Function_duration_seconds_count) by (functInvocationDuration)"},
		{"AvgOutputFunSize", "sum(FunctionOutput_size_seconds_sum) by (functionSizeHistogram) / sum(FunctionOutput_size_seconds_count) by (functionSizeHistogram)"},
		{"AvgRemoteFunDurationTime", "sum(Function_RemoteDuration_seconds_sum) by (functInvocationRemoteDuration) / sum(Function_RemoteDuration_seconds_count) by (functInvocationRemoteDuration)"},
		{"AvgRemoteOutputFunSize", "sum(FunctionOutput_RemoteSize_seconds_sum) by (functionRemoteSizeHistogram) / sum(FunctionOutput_RemoteSize_seconds_count) by (functionRemoteSizeHistogram)"},
		{"AvgFcRespTime", "sum(FunctionComposition_respTime_seconds_sum) by (functionCompositionInvocationRespTime) / clamp_min(sum(FunctionComposition_respTime_seconds_count) by (functionCompositionInvocationRespTime), 1)"},
		{"AvgFcRemoteRespTime", "sum(FunctionComposition_remoteRespTime_seconds_sum) by (functionCompositionInvocationRemoteRespTime) / clamp_min(sum(FunctionComposition_remoteRespTime_seconds_count) by (functionCompositionInvocationRemoteRespTime), 1)"},
		{"AvgSavingInfosTime", "sum(FunctionComposition_SavingInfosDuration_seconds_sum) by (functionCompositionSavingInfoDuration) / clamp_min(sum(FunctionComposition_SavingInfosDuration_seconds_count) by (functionCompositionSavingInfoDuration), 1)"},
		{"AvgFcTTranferTime", "sum(FunctionComposition_TTransferTime_seconds_sum) by (functionCompositionTTransferTime) / clamp_min(sum(FunctionComposition_TTransferTime_seconds_count) by (functionCompositionTTransferTime), 1)"},
		{"AvgFcTReturnTime", "sum(FunctionComposition_TReturnTime_seconds_sum) by (functionCompositionTReturnTime) / clamp_min(sum(FunctionComposition_TReturnTime_seconds_count) by (functionCompositionTReturnTime), 1)"},
		{"NoChoiceNodeInvocations", "sum by (fcChoiceNodeInvocationCounter) (ChoiceNode_InvocationNo_total)"},
		{"NoBranchInvocations", "sum by (fcBranchInvocationCounter) (BranchInvocations_total)"},
	}

	ticker := time.NewTicker(1500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			var wg sync.WaitGroup

			for _, query := range queries {
				wg.Add(1)
				byTerm := parserBy(query.query)
				go queryPrometheus(&wg, query, api, ctx, byTerm)
			}

			wg.Wait()
			fmt.Println("All queries completed")
			fcPolicyConf := config.GetString(config.SCHEDULING_FC_POLICY, "default")
			if fcPolicyConf == "greedyedgecloud" {
				cep := fc.GreedyCloudEdgePolicy{}
				cep.SubmitInfos(dataToSend)
			} else if fcPolicyConf == "dyngreedyedgecloud" {
				cep := fc.DynGreedyCloudEdgePolicy{}
				cep.SubmitInfos(dataToSend)
			} else if fcPolicyConf == "deadlinebased" {
				cep := fc.DeadlineCloudEdgePolicy{}
				cep.SubmitInfos(dataToSend)
			} else if fcPolicyConf == "dyndeadlinebased" {
				cep := fc.DynDeadlineCloudEdgePolicy{}
				cep.SubmitInfos(dataToSend)
			} else if fcPolicyConf == "thresholdbased" {
				cep := fc.ThresholdCloudEdgePolicy{}
				cep.SubmitInfos(dataToSend)
			} else if fcPolicyConf == "dynthresholdbased" {
				cep := fc.ThresholdDynCloudEdgePolicy{}
				cep.SubmitInfos(dataToSend)
			} else if fcPolicyConf == "cloudonly" {
				cep := fc.CloudOnlyPolicy{}
				cep.SubmitInfos(dataToSend)
			} else if fcPolicyConf == "edgeonly" {
				cep := fc.EdgeOnlyPolicy{}
				cep.SubmitInfos(dataToSend)
			}

			policyConf := config.GetString(config.SCHEDULING_POLICY, "default")
			if policyConf == "greedyedgecloud" {
				cep := scheduling.GreedyCloudEdgePolicy{}
				cep.SubmitInfos(dataFuncToSend)
			}
		}
	}
}

func ServerPromMetricsInit() {
	handler := promhttp.Handler()
	http.Handle("/metrics", handler)
	log.Println("Starting HTTP server on port 2112")

	if err := http.ListenAndServe(":2112", handler); err != nil {
		log.Fatal("Server failed to start:", err)
	}
}

func ServerMetricsInit() {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Hello world!"))
	})

	handler := otelhttp.NewHandler(mux, "/")

	log.Println("Starting HTTP server on port 8000")

	if err := http.ListenAndServe(":8000", handler); err != nil {
		log.Fatal("Server failed to start:", err)
	}
}
