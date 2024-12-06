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
	go_api "github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/model"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

var dataToSend fc.ReturnedOutputData

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
	case "AvgFcRespTime":
		dataToSend.AvgFcRespTime = outputMap
	case "AvgFunDurationTime":
		dataToSend.AvgFunDurationTime = outputMap
	case "AvgOutputFunSize":
		dataToSend.AvgOutputFunSize = outputMap
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
		{"AvgTotalColdStartsTime", "sum(ColdStart_duration_seconds_sum) / clamp_min(sum(ColdStart_duration_seconds_count),1)"},
		{"AvgFcRespTime", "sum(FunctionComposition_respTime_seconds_sum) by (functionCompositionInvocationRespTime) / clamp_min(sum(FunctionComposition_respTime_seconds_count) by (functionCompositionInvocationRespTime), 1)"},
		{"AvgFunDurationTime", "sum(Function_duration_seconds_sum) by (functInvocationCounter) / sum(Function_duration_seconds_count) by (functInvocationCounter)"},
		{"AvgOutputFunSize", "sum(FunctionOutput_size_seconds_sum) by (functionSizeHistogram) / sum(FunctionOutput_size_seconds_count) by (functionSizeHistogram)"},
	}
	ticker := time.NewTicker(20 * time.Second)
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
			policyConf := config.GetString(config.SCHEDULING_FC_POLICY, "default")
			if policyConf == "edgecloud" {
				cep := fc.CloudEdgePolicy{}
				cep.SubmitInfos(dataToSend)
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
