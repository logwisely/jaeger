// Copyright (c) 2025 The Jaeger Authors.
// SPDX-License-Identifier: Apache-2.0

package tracestoremetrics

import (
	"context"
	"iter"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/jaegertracing/jaeger/internal/metricstest"
	"github.com/jaegertracing/jaeger/internal/storage/v2/api/tracestore"
	"github.com/jaegertracing/jaeger/internal/storage/v2/api/tracestore/mocks"
)

type indexedAttributesReader struct {
	tracestore.Reader
	onGetIndexedAttributesNames func(context.Context, tracestore.IndexedAttributesNamesQueryParams) ([]string, error)
	onGetTopKAttributeValues    func(context.Context, tracestore.KAttributeValuesQueryParams) ([]string, error)
	onGetBottomKAttributeValues func(context.Context, tracestore.KAttributeValuesQueryParams) ([]string, error)
}

func (r *indexedAttributesReader) GetIndexedAttributesNames(
	ctx context.Context,
	query tracestore.IndexedAttributesNamesQueryParams,
) ([]string, error) {
	return r.onGetIndexedAttributesNames(ctx, query)
}

func (r *indexedAttributesReader) GetTopKAttributeValues(
	ctx context.Context,
	query tracestore.KAttributeValuesQueryParams,
) ([]string, error) {
	return r.onGetTopKAttributeValues(ctx, query)
}

func (r *indexedAttributesReader) GetBottomKAttributeValues(
	ctx context.Context,
	query tracestore.KAttributeValuesQueryParams,
) ([]string, error) {
	return r.onGetBottomKAttributeValues(ctx, query)
}

func TestSuccessfulUnderlyingCalls(t *testing.T) {
	mf := metricstest.NewFactory(0)

	mockReader := mocks.Reader{}
	mrs := NewReaderDecorator(&mockReader, mf)
	traces := []ptrace.Traces{ptrace.NewTraces(), ptrace.NewTraces()}
	mockReader.On("GetServices", context.Background()).Return([]string{"service-x"}, nil)
	mrs.GetServices(context.Background())
	operationQuery := tracestore.OperationQueryParams{ServiceName: "something"}
	mockReader.On("GetOperations", context.Background(), operationQuery).
		Return([]tracestore.Operation{{}}, nil)
	mrs.GetOperations(context.Background(), operationQuery)
	mockReader.On("GetTraces", context.Background(), []tracestore.GetTraceParams{{}}).Return(emptyIter[ptrace.Traces](traces, nil))
	count := 0
	for range mrs.GetTraces(context.Background(), tracestore.GetTraceParams{}) {
		if count != 0 {
			break
		}
		count++
	}
	mockReader.On("FindTraces", context.Background(), tracestore.TraceQueryParams{}).
		Return(emptyIter[ptrace.Traces](traces, nil))
	count = 0
	for range mrs.FindTraces(context.Background(), tracestore.TraceQueryParams{}) {
		if count != 0 {
			break
		}
		count++
	}
	mockReader.On("FindTraceIDs", context.Background(), tracestore.TraceQueryParams{}).
		Return(emptyIter[tracestore.FoundTraceID]([]tracestore.FoundTraceID{{TraceID: [16]byte{}}, {TraceID: [16]byte{}}}, nil))
	count = 0
	for range mrs.FindTraceIDs(context.Background(), tracestore.TraceQueryParams{}) {
		if count != 0 {
			break
		}
		count++
	}
	counters, gauges := mf.Snapshot()
	expected := map[string]int64{
		"requests|operation=get_operations|result=ok":  1,
		"requests|operation=get_operations|result=err": 0,
		"requests|operation=get_trace|result=ok":       1,
		"requests|operation=get_trace|result=err":      0,
		"requests|operation=find_traces|result=ok":     1,
		"requests|operation=find_traces|result=err":    0,
		"requests|operation=find_trace_ids|result=ok":  1,
		"requests|operation=find_trace_ids|result=err": 0,
		"requests|operation=get_services|result=ok":    1,
		"requests|operation=get_services|result=err":   0,
		"responses|operation=get_trace":                2,
		"responses|operation=find_traces":              2,
		"responses|operation=find_trace_ids":           2,
		"responses|operation=get_operations":           1,
		"responses|operation=get_services":             1,
	}

	existingKeys := []string{
		"latency|operation=get_operations|result=ok.P50",
		"latency|operation=find_traces|result=ok.P50", // this is not exhaustive
	}
	nonExistentKeys := []string{
		"latency|operation=get_operations|result=err.P50",
	}

	checkExpectedExistingAndNonExistentCounters(t, counters, expected, gauges, existingKeys, nonExistentKeys)
}

func checkExpectedExistingAndNonExistentCounters(t *testing.T,
	actualCounters,
	expectedCounters,
	actualGauges map[string]int64,
	existingKeys,
	nonExistentKeys []string,
) {
	for k, v := range expectedCounters {
		assert.Equal(t, v, actualCounters[k], k)
	}

	for _, k := range existingKeys {
		_, ok := actualGauges[k]
		assert.True(t, ok, k)
	}

	for _, k := range nonExistentKeys {
		_, ok := actualGauges[k]
		assert.False(t, ok, k)
	}
}

func TestFailingUnderlyingCalls(t *testing.T) {
	mf := metricstest.NewFactory(0)

	mockReader := mocks.Reader{}
	mrs := NewReaderDecorator(&mockReader, mf)
	returningErr := assert.AnError
	mockReader.On("GetServices", context.Background()).
		Return(nil, returningErr)
	mrs.GetServices(context.Background())
	operationQuery := tracestore.OperationQueryParams{ServiceName: "something"}
	mockReader.On("GetOperations", context.Background(), operationQuery).
		Return(nil, returningErr)
	mrs.GetOperations(context.Background(), operationQuery)
	mockReader.On("GetTraces", context.Background(), []tracestore.GetTraceParams{{}}).
		Return(emptyIter[ptrace.Traces](nil, returningErr))
	for range mrs.GetTraces(context.Background(), tracestore.GetTraceParams{}) {
		t.Log("GetTraces iteration")
	}
	mockReader.On("FindTraces", context.Background(), tracestore.TraceQueryParams{}).
		Return(emptyIter[ptrace.Traces](nil, returningErr))
	for range mrs.FindTraces(context.Background(), tracestore.TraceQueryParams{}) {
		t.Log("FindTraces iteration")
	}
	mockReader.On("FindTraceIDs", context.Background(), tracestore.TraceQueryParams{}).
		Return(emptyIter[tracestore.FoundTraceID](nil, returningErr))
	for range mrs.FindTraceIDs(context.Background(), tracestore.TraceQueryParams{}) {
		t.Log("FindTraceIDs iteration")
	}
	counters, gauges := mf.Snapshot()
	expecteds := map[string]int64{
		"requests|operation=get_operations|result=ok":  0,
		"requests|operation=get_operations|result=err": 1,
		"requests|operation=get_trace|result=ok":       0,
		"requests|operation=get_trace|result=err":      1,
		"requests|operation=find_traces|result=ok":     0,
		"requests|operation=find_traces|result=err":    1,
		"requests|operation=find_trace_ids|result=ok":  0,
		"requests|operation=find_trace_ids|result=err": 1,
		"requests|operation=get_services|result=ok":    0,
		"requests|operation=get_services|result=err":   1,
	}

	existingKeys := []string{
		"latency|operation=get_operations|result=err.P50",
	}

	nonExistentKeys := []string{
		"latency|operation=get_operations|result=ok.P50",
		"latency|operation=query|result=ok.P50", // this is not exhaustive
	}

	checkExpectedExistingAndNonExistentCounters(t, counters, expecteds, gauges, existingKeys, nonExistentKeys)
}

func TestGetIndexedAttributesNames(t *testing.T) {
	mockReader := &mocks.Reader{}
	var actualQuery tracestore.IndexedAttributesNamesQueryParams
	reader := &indexedAttributesReader{
		Reader: mockReader,
		onGetIndexedAttributesNames: func(_ context.Context, query tracestore.IndexedAttributesNamesQueryParams) ([]string, error) {
			actualQuery = query
			return []string{"http.method"}, nil
		},
	}
	mrs := NewReaderDecorator(reader, metricstest.NewFactory(0))
	expectedQuery := tracestore.IndexedAttributesNamesQueryParams{
		Query: tracestore.TraceQueryParams{
			ServiceName: "frontend",
		},
		Limit: 5,
	}

	names, err := mrs.GetIndexedAttributesNames(context.Background(), expectedQuery)

	assert.NoError(t, err)
	assert.Equal(t, []string{"http.method"}, names)
	assert.Equal(t, expectedQuery, actualQuery)
}

func TestGetIndexedAttributesNamesNotSupported(t *testing.T) {
	mockReader := &mocks.Reader{}
	mrs := NewReaderDecorator(mockReader, metricstest.NewFactory(0))

	names, err := mrs.GetIndexedAttributesNames(
		context.Background(),
		tracestore.IndexedAttributesNamesQueryParams{},
	)

	assert.ErrorIs(t, err, tracestore.ErrIndexedAttributesNamesNotSupported)
	assert.Nil(t, names)
}

func TestGetTopKAttributeValues(t *testing.T) {
	mockReader := &mocks.Reader{}
	var actualQuery tracestore.KAttributeValuesQueryParams
	reader := &indexedAttributesReader{
		Reader: mockReader,
		onGetTopKAttributeValues: func(_ context.Context, query tracestore.KAttributeValuesQueryParams) ([]string, error) {
			actualQuery = query
			return []string{"GET"}, nil
		},
		onGetBottomKAttributeValues: func(context.Context, tracestore.KAttributeValuesQueryParams) ([]string, error) {
			return nil, assert.AnError
		},
	}
	mrs := NewReaderDecorator(reader, metricstest.NewFactory(0))
	expectedQuery := tracestore.KAttributeValuesQueryParams{
		Query: tracestore.TraceQueryParams{
			ServiceName: "frontend",
		},
		AttributeName: "http.method",
		K:             3,
	}

	values, err := mrs.GetTopKAttributeValues(context.Background(), expectedQuery)

	assert.NoError(t, err)
	assert.Equal(t, []string{"GET"}, values)
	assert.Equal(t, expectedQuery, actualQuery)
}

func TestGetTopKAttributeValuesNotSupported(t *testing.T) {
	mockReader := &mocks.Reader{}
	mrs := NewReaderDecorator(mockReader, metricstest.NewFactory(0))

	values, err := mrs.GetTopKAttributeValues(
		context.Background(),
		tracestore.KAttributeValuesQueryParams{},
	)

	assert.ErrorIs(t, err, tracestore.ErrAttributeValuesQueryNotSupported)
	assert.Nil(t, values)
}

func TestGetBottomKAttributeValues(t *testing.T) {
	mockReader := &mocks.Reader{}
	var actualQuery tracestore.KAttributeValuesQueryParams
	reader := &indexedAttributesReader{
		Reader: mockReader,
		onGetTopKAttributeValues: func(context.Context, tracestore.KAttributeValuesQueryParams) ([]string, error) {
			return nil, assert.AnError
		},
		onGetBottomKAttributeValues: func(_ context.Context, query tracestore.KAttributeValuesQueryParams) ([]string, error) {
			actualQuery = query
			return []string{"DELETE"}, nil
		},
	}
	mrs := NewReaderDecorator(reader, metricstest.NewFactory(0))
	expectedQuery := tracestore.KAttributeValuesQueryParams{
		Query: tracestore.TraceQueryParams{
			ServiceName: "frontend",
		},
		AttributeName: "http.method",
		K:             2,
	}

	values, err := mrs.GetBottomKAttributeValues(context.Background(), expectedQuery)

	assert.NoError(t, err)
	assert.Equal(t, []string{"DELETE"}, values)
	assert.Equal(t, expectedQuery, actualQuery)
}

func TestGetBottomKAttributeValuesNotSupported(t *testing.T) {
	mockReader := &mocks.Reader{}
	mrs := NewReaderDecorator(mockReader, metricstest.NewFactory(0))

	values, err := mrs.GetBottomKAttributeValues(
		context.Background(),
		tracestore.KAttributeValuesQueryParams{},
	)

	assert.ErrorIs(t, err, tracestore.ErrAttributeValuesQueryNotSupported)
	assert.Nil(t, values)
}

func emptyIter[T any](td []T, err error) iter.Seq2[[]T, error] {
	return func(yield func([]T, error) bool) {
		if err != nil {
			yield(nil, err)
			return
		}
		for _, t := range td {
			if !yield([]T{t}, nil) {
				return
			}
		}
	}
}
