// Copyright (c) 2026 The Jaeger Authors.
// SPDX-License-Identifier: Apache-2.0

package memory

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/jaegertracing/jaeger/internal/storage/v2/api/attrstore"
	conventions "github.com/jaegertracing/jaeger/internal/telemetry/otelsemconv"
)

func TestStore_GetIndexedAttributesNames(t *testing.T) {
	store, err := NewStore(Configuration{MaxTraces: 10})
	require.NoError(t, err)

	ctx := context.Background()
	td := loadInputTraces(t, 1)
	require.NoError(t, store.WriteTraces(ctx, td))

	params := attrstore.GetIndexedAttributesNamesParams{
		ServiceName:   "service-x",
		OperationName: "test-general-conversion-2",
	}
	names, err := store.GetIndexedAttributesNames(ctx, params)
	require.NoError(t, err)
	assert.NotEmpty(t, names)

	assert.Contains(t, names, "peer.service")
	assert.Contains(t, names, "temperature")
	assert.Contains(t, names, "event-x")
	assert.Contains(t, names, "scope.attributes.2")
	assert.Contains(t, names, "service.name")
}

func TestStore_GetIndexedAttributesNames_Limit(t *testing.T) {
	store, err := NewStore(Configuration{MaxTraces: 10})
	require.NoError(t, err)

	ctx := context.Background()
	td := loadInputTraces(t, 1)
	require.NoError(t, store.WriteTraces(ctx, td))

	names, err := store.GetIndexedAttributesNames(ctx, attrstore.GetIndexedAttributesNamesParams{Limit: 1})
	require.NoError(t, err)
	assert.Len(t, names, 1)
}

func TestStore_GetIndexedAttributesNames_UnknownService(t *testing.T) {
	store, err := NewStore(Configuration{MaxTraces: 10})
	require.NoError(t, err)

	ctx := context.Background()
	td := loadInputTraces(t, 1)
	require.NoError(t, store.WriteTraces(ctx, td))

	names, err := store.GetIndexedAttributesNames(ctx, attrstore.GetIndexedAttributesNamesParams{ServiceName: "does-not-exist"})
	require.NoError(t, err)
	assert.Empty(t, names)
}

func TestStore_GetTopKAttributeValues_StringOnlyAndFilters(t *testing.T) {
	store, err := NewStore(Configuration{MaxTraces: 20})
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, writeTopKTestTrace(ctx, store, 1, "svc-a", "op-a", "http.method", "GET", true))
	require.NoError(t, writeTopKTestTrace(ctx, store, 2, "svc-a", "op-a", "http.method", "GET", false))
	require.NoError(t, writeTopKTestTrace(ctx, store, 3, "svc-a", "op-b", "http.method", "POST", false))
	require.NoError(t, writeTopKTestTrace(ctx, store, 4, "svc-b", "op-a", "http.method", "PUT", false))

	values, err := store.GetTopKAttributeValues(ctx, attrstore.GetTopKAttributeValuesParams{
		ServiceName:   "svc-a",
		OperationName: "op-a",
		AttributeName: "http.method",
		K:             2,
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"GET"}, values)

	values, err = store.GetTopKAttributeValues(ctx, attrstore.GetTopKAttributeValuesParams{
		ServiceName:   "svc-a",
		OperationName: "op-a",
		AttributeName: "http.status_code",
		K:             2,
	})
	require.NoError(t, err)
	assert.Empty(t, values)
}

func TestStore_GetTopKAttributeValues_UsesMostRecent100Traces(t *testing.T) {
	store, err := NewStore(Configuration{MaxTraces: 200})
	require.NoError(t, err)

	ctx := context.Background()
	for i := 1; i <= 50; i++ {
		require.NoError(t, writeTopKTestTrace(ctx, store, i, "svc", "op", "http.method", "zzz-old", false))
	}
	for i := 51; i <= 90; i++ {
		require.NoError(t, writeTopKTestTrace(ctx, store, i, "svc", "op", "http.method", "new-a", false))
	}
	for i := 91; i <= 125; i++ {
		require.NoError(t, writeTopKTestTrace(ctx, store, i, "svc", "op", "http.method", "new-b", false))
	}
	for i := 126; i <= 150; i++ {
		require.NoError(t, writeTopKTestTrace(ctx, store, i, "svc", "op", "http.method", "new-c", false))
	}

	values, err := store.GetTopKAttributeValues(ctx, attrstore.GetTopKAttributeValuesParams{
		ServiceName:   "svc",
		OperationName: "op",
		AttributeName: "http.method",
		K:             1,
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"new-a"}, values)
}

func writeTopKTestTrace(
	ctx context.Context,
	store *Store,
	seq int,
	service string,
	operation string,
	attrName string,
	attrValue string,
	withNumeric bool,
) error {
	td := ptrace.NewTraces()
	resourceSpan := td.ResourceSpans().AppendEmpty()
	resourceSpan.Resource().Attributes().PutStr(conventions.ServiceNameKey, service)
	span := resourceSpan.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	var traceID pcommon.TraceID
	binary.BigEndian.PutUint64(traceID[8:], uint64(seq))
	span.SetTraceID(traceID)
	span.SetName(operation)
	startTime := time.Unix(int64(seq), 0)
	span.SetStartTimestamp(pcommon.NewTimestampFromTime(startTime))
	span.SetEndTimestamp(pcommon.NewTimestampFromTime(startTime.Add(time.Second)))
	span.Attributes().PutStr(attrName, attrValue)
	if withNumeric {
		span.Attributes().PutInt("http.status_code", 200)
	}
	return store.WriteTraces(ctx, td)
}
