package utils

import (
	"context"
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

func CreateAndRecordNewCounterMetric(reqId string, attributeKey string, attributeValue string) {
	meter := otel.Meter(os.Getenv("OTEL_SERVICE_NAME"))
	m, err := telemetry.NewCounterMetric(meter)
	if err != nil {
		panic(err)
	}

	m.RequestCounter.Add(
		context.WithValue(context.Background(), "ReqId", reqId),
		1,
		metric.WithAttributes(attribute.String(attributeKey, attributeValue)),
	)
}
