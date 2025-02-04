package utils

import (
	"context"
	"fmt"
	"os"

	"github.com/grussorusso/serverledge/internal/telemetry"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

func CreateAndRecordNewHistogramMetric(metricName string, metricDesc string, context context.Context, value float64, attributeKey string, attributeValue string) {
	meter := otel.Meter(os.Getenv("OTEL_SERVICE_NAME"))
	m, err := telemetry.NewHistogramMetric(meter, metricName, metricDesc)
	if err != nil {
		panic(err)
	}

	m.Record(
		context,
		value,
		metric.WithAttributes(attribute.String(attributeKey, attributeValue)))
}

func CreateAndRecordNewCounterMetric(metricName string, metricDesc string, ctx context.Context, attributeKey string, attributeValue string) {
	meter := otel.Meter(os.Getenv("OTEL_SERVICE_NAME"))
	m, err := telemetry.NewCounterMetric(meter, metricName, metricDesc)
	if err != nil {
		panic(err)
	}

	m.RequestCounter.Add(
		ctx,
		1,
		metric.WithAttributes(attribute.String(attributeKey, attributeValue)),
	)
}

func FormatParams(params map[string]interface{}) string {
	if len(params) == 0 {
		return "empty"
	}

	result := ""
	first := true
	for _, v := range params {
		if !first {
			result += ", "
		}
		result += fmt.Sprintf("%v", v)
		first = false
	}
	return result
}
