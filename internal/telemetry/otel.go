package telemetry

import (
	"context"
	"errors"
	"log"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	stdmtr "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

var DefaultTracer trace.Tracer = nil
var MetricsEnabled bool = false

type spanWriter struct {
	file        *os.File
	isFirstSpan *bool
}

func (w *spanWriter) Write(p []byte) (n int, err error) {
	if !*w.isFirstSpan {
		// Aggiunge una virgola prima di scrivere un nuovo span, eccetto il primo
		if _, err := w.file.WriteString(",\n"); err != nil {
			return 0, err
		}
	}
	*w.isFirstSpan = false // Dopo il primo span, attiviamo la virgola
	return w.file.Write(p)
}

// setupOTelSDK bootstraps the OpenTelemetry pipeline.
// If it does not return an error, make sure to call shutdown for proper cleanup.

func SetupOTelSDK(ctx context.Context, outputFilename string) (shutdown func(context.Context) error, err error) {
	var shutdownFuncs []func(context.Context) error
	// shutdown calls cleanup functions registered via shutdownFuncs.
	// The errors from the calls are joined.
	// Each registered cleanup will be invoked once.
	shutdown = func(ctx context.Context) error {
		var err error
		for _, fn := range shutdownFuncs {
			err = errors.Join(err, fn(ctx))
		}
		shutdownFuncs = nil
		return err
	}
	// handleErr calls shutdown for cleanup and makes sure that all errors are returned.
	handleErr := func(inErr error) {
		err = errors.Join(inErr, shutdown(ctx))
	}
	// Set up propagator.
	prop := newPropagator()
	otel.SetTextMapPropagator(prop)
	// Set up trace provider.
	f, err := os.OpenFile(outputFilename, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		handleErr(err)
		return
	}

	// Scrivi il carattere di apertura dell'array
	_, err = f.WriteString("[\n")
	if err != nil {
		handleErr(err)
		return
	}

	isFirstSpan := true

	//traceExporter, err := stdouttrace.New(stdouttrace.WithWriter(f), stdouttrace.WithPrettyPrint())
	traceExporter, err := stdouttrace.New(
		stdouttrace.WithWriter(&spanWriter{
			file:        f,
			isFirstSpan: &isFirstSpan, // Passiamo un puntatore al flag
		}),
		stdouttrace.WithPrettyPrint(),
	)
	if err != nil {
		handleErr(err)
		return
	}

	shutdownFuncs = append(shutdownFuncs, func(ctx context.Context) error {
		// Chiudi l'array JSON
		_, closeErr := f.WriteString("\n]\n")
		if closeErr != nil {
			return closeErr
		}
		return f.Close()
	})
	tracerProvider := sdktrace.NewTracerProvider(sdktrace.WithBatcher(traceExporter))

	shutdownFuncs = append(shutdownFuncs, tracerProvider.Shutdown)
	otel.SetTracerProvider(tracerProvider)
	// Finally, set the tracer that can be used for this package.
	DefaultTracer = tracerProvider.Tracer("github.com/creccovalerio/serverledge")
	// NOTE: could boostrap metric provider as well
	return
}

func SetupOTelMetricsSDK(ctx context.Context) (shutdown func(context.Context) error, err error) {
	var shutdownFuncs []func(context.Context) error
	// shutdown calls cleanup functions registered via shutdownFuncs.
	// The errors from the calls are joined.
	// Each registered cleanup will be invoked once.

	shutdown = func(ctx context.Context) error {
		var err error

		for _, fn := range shutdownFuncs {
			err = errors.Join(err, fn(ctx))
		}

		shutdownFuncs = nil

		return err
	}

	handleErr := func(inErr error) {
		err = errors.Join(inErr, shutdown(ctx))
	}

	/*
		res, err := newResource()
		if err != nil {
			handleErr(err)
			return
		}*/

	/* change this call with newMeterProvider(...) to use stdout as exporter
	 * and uncomment the newResource() call */
	meterProvider, err := newPrometheusMeterProvider()
	if err != nil {
		handleErr(err)
		return
	}

	shutdownFuncs = append(shutdownFuncs, meterProvider.Shutdown)

	otel.SetMeterProvider(meterProvider)

	return
}

func newPropagator() propagation.TextMapPropagator {
	return propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	)
}

/*
func newResource() (*resource.Resource, error) {
	return resource.Merge(resource.Default(),
		resource.NewWithAttributes(semconv.SchemaURL,
			semconv.ServiceName("my-service"),
			semconv.ServiceVersion("0.1.0"),
		))
}*/

func newMeterProvider(ctx context.Context, res *resource.Resource) (*metric.MeterProvider, error) {
	metricExporter, err := stdoutmetric.New(stdoutmetric.WithPrettyPrint())
	if err != nil {
		return nil, err
	}

	MetricsEnabled = true
	meterProvider := metric.NewMeterProvider(
		metric.WithResource(res),
		metric.WithReader(metric.NewPeriodicReader(metricExporter,
			// Default is 1m. Set to 3s for demonstrative purposes.
			metric.WithInterval(30*time.Second))),
	)

	return meterProvider, nil
}

func newPrometheusMeterProvider() (*metric.MeterProvider, error) {
	metricExporter, err := prometheus.New()
	if err != nil {
		log.Fatal(err)
	}

	MetricsEnabled = true
	meterProvider := metric.NewMeterProvider(metric.WithReader(metricExporter))

	return meterProvider, nil
}

type metrics struct {
	RequestCounter stdmtr.Int64Counter
}

func NewCounterMetric(meter stdmtr.Meter, name string, desc string) (*metrics, error) {

	var m metrics
	newMetric, err := meter.Int64Counter(
		name,
		stdmtr.WithDescription(desc),
		stdmtr.WithUnit("{requests}"),
	)
	if err != nil {
		return nil, err
	}
	m.RequestCounter = newMetric
	return &m, nil

}

func NewHistogramMetric(meter stdmtr.Meter, name string, desc string) (stdmtr.Float64Histogram, error) {

	newHistogramMetric, err := meter.Float64Histogram(
		name,
		stdmtr.WithDescription(desc),
		stdmtr.WithUnit("s"),
	)
	if err != nil {
		return nil, err
	}

	return newHistogramMetric, nil

}
