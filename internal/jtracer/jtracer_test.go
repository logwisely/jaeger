// Copyright (c) 2023 The Jaeger Authors.
// SPDX-License-Identifier: Apache-2.0

package jtracer

import (
	"context"
	"encoding/hex"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/jaegertracing/jaeger/internal/testutils"
)

func TestNewProvider(t *testing.T) {
	p, c, err := NewProvider(t.Context(), "serviceName")
	require.NoError(t, err)
	require.NotNil(t, p, "Expected OTEL not to be nil")
	require.NotNil(t, c, "Expected closer not to be nil")
	c(t.Context())
}

func TestNewHelperProviderError(t *testing.T) {
	fakeErr := errors.New("fakeProviderError")
	_, _, err := newProviderHelper(
		t.Context(),
		"svc",
		func(_ context.Context, _ /* svc */ string) (*sdktrace.TracerProvider, error) {
			return nil, fakeErr
		})
	require.Error(t, err)
	require.EqualError(t, err, fakeErr.Error())
}

func TestInitHelperExporterError(t *testing.T) {
	fakeErr := errors.New("fakeExporterError")
	_, err := initHelper(
		context.Background(),
		"svc",
		func(_ context.Context) (sdktrace.SpanExporter, error) {
			return nil, fakeErr
		},
		func(_ context.Context, _ /* svc */ string) (*resource.Resource, error) {
			return nil, nil
		},
	)
	require.Error(t, err)
	require.EqualError(t, err, fakeErr.Error())
}

func TestInitHelperResourceError(t *testing.T) {
	fakeErr := errors.New("fakeResourceError")
	tp, err := initHelper(
		context.Background(),
		"svc",
		otelExporter,
		func(_ context.Context, _ /* svc */ string) (*resource.Resource, error) {
			return nil, fakeErr
		},
	)
	require.Error(t, err)
	require.Nil(t, tp)
	require.EqualError(t, err, fakeErr.Error())
}

func TestInitHelperUsesXRayIDGenerator(t *testing.T) {
	tp, err := initHelper(
		context.Background(),
		"svc",
		func(_ context.Context) (sdktrace.SpanExporter, error) {
			return tracetest.NewNoopExporter(), nil
		},
		func(_ context.Context, _ /* svc */ string) (*resource.Resource, error) {
			return resource.Empty(), nil
		},
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, tp.Shutdown(context.Background()))
	}()

	_, span := tp.Tracer("jtracer-test").Start(context.Background(), "span")
	defer span.End()

	traceID := span.SpanContext().TraceID()
	require.True(t, traceID.IsValid())

	tsHex := hex.EncodeToString(traceID[:4])
	ts, err := strconv.ParseInt(tsHex, 16, 64)
	require.NoError(t, err)
	require.WithinDuration(t, time.Now(), time.Unix(ts, 0), 10*time.Second)
}

func TestMain(m *testing.M) {
	testutils.VerifyGoLeaks(m)
}
