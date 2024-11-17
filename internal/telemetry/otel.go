package telemetry

import (
	"context"
	"errors"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

var DefaultTracer trace.Tracer = nil

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
func newPropagator() propagation.TextMapPropagator {
	return propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	)
}
