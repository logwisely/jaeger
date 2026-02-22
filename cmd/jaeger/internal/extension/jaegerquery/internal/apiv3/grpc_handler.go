// Copyright (c) 2021 The Jaeger Authors.
// SPDX-License-Identifier: Apache-2.0

package apiv3

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"sort"
	"time"

	"go.opentelemetry.io/collector/pdata/ptrace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/jaegertracing/jaeger-idl/model/v1"
	"github.com/jaegertracing/jaeger/cmd/jaeger/internal/extension/jaegerquery/querysvc"
	"github.com/jaegertracing/jaeger/internal/jiter"
	"github.com/jaegertracing/jaeger/internal/jptrace"
	"github.com/jaegertracing/jaeger/internal/proto/api_v3"
	"github.com/jaegertracing/jaeger/internal/storage/v2/api/tracestore"
	"github.com/jaegertracing/jaeger/internal/storage/v2/v1adapter"
)

// Handler implements api_v3.QueryServiceServer
type Handler struct {
	QueryService *querysvc.QueryService
}

// remove me
var _ api_v3.QueryServiceServer = (*Handler)(nil)

// GetTrace implements api_v3.QueryServiceServer's GetTrace
func (h *Handler) GetTrace(
	request *api_v3.GetTraceRequest,
	stream api_v3.QueryService_GetTraceServer,
) error {
	traceID, err := model.TraceIDFromString(request.GetTraceId())
	if err != nil {
		return fmt.Errorf("malform trace ID: %w", err)
	}

	query := querysvc.GetTraceParams{
		TraceIDs: []tracestore.GetTraceParams{
			{
				TraceID: v1adapter.FromV1TraceID(traceID),
				Start:   request.GetStartTime(),
				End:     request.GetEndTime(),
			},
		},
		RawTraces: request.GetRawTraces(),
	}
	getTracesIter := h.QueryService.GetTraces(stream.Context(), query)
	return receiveTraces(getTracesIter, stream.Send)
}

// FindTraces implements api_v3.QueryServiceServer's FindTraces
func (h *Handler) FindTraces(
	request *api_v3.FindTracesRequest,
	stream api_v3.QueryService_FindTracesServer,
) error {
	return h.internalFindTraces(stream.Context(), request, stream.Send)
}

// separated for testing
func (h *Handler) internalFindTraces(
	ctx context.Context,
	request *api_v3.FindTracesRequest,
	streamSend func(*jptrace.TracesData) error,
) error {
	query := request.GetQuery()
	if query == nil {
		return status.Error(codes.InvalidArgument, "missing query")
	}
	if query.GetStartTimeMin().IsZero() ||
		query.GetStartTimeMax().IsZero() {
		return errors.New("start time min and max are required parameters")
	}

	queryParams := querysvc.TraceQueryParams{
		TraceQueryParams: tracestore.TraceQueryParams{
			ServiceName:   query.GetServiceName(),
			OperationName: query.GetOperationName(),
			Attributes:    jptrace.PlainMapToPcommonMap(query.GetAttributes()),
			SearchDepth:   int(query.GetSearchDepth()),
		},
		RawTraces: query.GetRawTraces(),
	}
	if ts := query.GetStartTimeMin(); !ts.IsZero() {
		queryParams.StartTimeMin = ts
	}
	if ts := query.GetStartTimeMax(); !ts.IsZero() {
		queryParams.StartTimeMax = ts
	}
	if d := query.GetDurationMin(); d != 0 {
		queryParams.DurationMin = d
	}
	if d := query.GetDurationMax(); d != 0 {
		queryParams.DurationMax = d
	}

	findTracesIter := h.QueryService.FindTraces(ctx, queryParams)
	return receiveTraces(findTracesIter, streamSend)
}

// GetServices implements api_v3.QueryServiceServer's GetServices
func (h *Handler) GetServices(
	ctx context.Context,
	_ *api_v3.GetServicesRequest,
) (*api_v3.GetServicesResponse, error) {
	services, err := h.QueryService.GetServices(ctx)
	if err != nil {
		return nil, err
	}
	return &api_v3.GetServicesResponse{
		Services: services,
	}, nil
}

// GetOperations implements api_v3.QueryService's GetOperations
func (h *Handler) GetOperations(
	ctx context.Context,
	request *api_v3.GetOperationsRequest,
) (*api_v3.GetOperationsResponse, error) {
	operations, err := h.QueryService.GetOperations(
		ctx,
		tracestore.OperationQueryParams{
			ServiceName: request.GetService(),
			SpanKind:    request.GetSpanKind(),
		},
	)
	if err != nil {
		return nil, err
	}
	apiOperations := make([]*api_v3.Operation, len(operations))
	for i := range operations {
		apiOperations[i] = &api_v3.Operation{
			Name:     operations[i].Name,
			SpanKind: operations[i].SpanKind,
		}
	}
	return &api_v3.GetOperationsResponse{
		Operations: apiOperations,
	}, nil
}

// GetDependencies implements api_v3.QueryService's GetDependencies.
func (h *Handler) GetDependencies(
	ctx context.Context,
	request *api_v3.GetDependenciesRequest,
) (*api_v3.DependenciesResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "missing request")
	}
	startTime := request.GetStartTime()
	endTime := request.GetEndTime()
	if startTime.IsZero() || endTime.IsZero() {
		return nil, status.Error(
			codes.InvalidArgument,
			"start_time and end_time are required",
		)
	}

	deps, err := h.QueryService.GetDependencies(
		ctx,
		endTime,
		endTime.Sub(startTime),
	)
	if err != nil {
		return nil, err
	}
	apiDeps := make([]*api_v3.Dependency, len(deps))
	for i := range deps {
		apiDeps[i] = &api_v3.Dependency{
			Parent:    deps[i].Parent,
			Child:     deps[i].Child,
			CallCount: deps[i].CallCount,
		}
	}
	return &api_v3.DependenciesResponse{Dependencies: apiDeps}, nil
}

// GetIndexedAttributeNames implements api_v3.QueryService's GetIndexedAttributeNames.
func (h *Handler) GetIndexedAttributesNames(
	ctx context.Context,
	request *api_v3.GetIndexedAttributesNamesRequest,
) (*api_v3.GetAttributesNamesResponse, error) {
	query := request.GetQuery()
	if query == nil {
		return nil, status.Error(codes.InvalidArgument, "missing query")
	}
	if request.GetServiceName() == "" {
		return nil, status.Error(
			codes.InvalidArgument,
			"service_name is required",
		)
	}
	searchDepth := int(query.GetSearchDepth())
	if searchDepth <= 0 {
		searchDepth = defaultAttributeSuggestionSearchDepth
	}
	limit := int(request.GetLimit())
	if limit <= 0 {
		limit = defaultAttributeNamesLimit
	}

	queryParams := querysvc.TraceQueryParams{
		TraceQueryParams: tracestore.TraceQueryParams{
			ServiceName:   query.GetServiceName(),
			OperationName: query.GetOperationName(),
			Attributes:    jptrace.PlainMapToPcommonMap(query.GetAttributes()),
			SearchDepth:   searchDepth,
			StartTimeMin:  query.GetStartTimeMin(),
			StartTimeMax:  query.GetStartTimeMax(),
			DurationMin:   time.Duration(query.GetDurationMin()),
			DurationMax:   time.Duration(query.GetDurationMax()),
		},
		RawTraces: query.GetRawTraces(),
	}

	findTracesIter := h.QueryService.FindTraces(ctx, queryParams)
	traces, err := jiter.FlattenWithErrors(findTracesIter)
	if err != nil {
		return nil, err
	}
	names := collectAttributeNames(traces)
	sort.Strings(names)
	if len(names) > limit {
		names = names[:limit]
	}
	return &api_v3.GetAttributesNamesResponse{Names: names}, nil
}

// GetTopKAttributeValues implements api_v3.QueryService's GetTopKAttributeValues.
func (h *Handler) GetTopKAttributeValues(
	ctx context.Context,
	request *api_v3.GetTopKAttributeValuesRequest,
) (*api_v3.GetTopKAttributeValuesResponse, error) {
	resp, err := h.getKAttributeValues(
		ctx,
		request.GetQuery(),
		request.GetAttributeName(),
		int(request.GetK()),
		true,
	)
	if err != nil {
		return nil, err
	}
	return &api_v3.GetTopKAttributeValuesResponse{Values: resp}, nil
}

// GetBottomKAttributeValues implements api_v3.QueryService's GetBottomKAttributeValues.
func (h *Handler) GetBottomKAttributeValues(
	ctx context.Context,
	request *api_v3.GetBottomKAttributeValuesRequest,
) (*api_v3.GetBottomKAttributeValuesResponse, error) {
	resp, err := h.getKAttributeValues(
		ctx,
		request.GetQuery(),
		request.GetAttributeName(),
		int(request.GetK()),
		false,
	)
	if err != nil {
		return nil, err
	}
	return &api_v3.GetBottomKAttributeValuesResponse{Values: resp}, nil
}

func (h *Handler) getKAttributeValues(
	ctx context.Context,
	query *api_v3.TraceQueryParameters,
	attributeName string,
	k int,
	desc bool,
) ([]string, error) {
	if query == nil {
		return nil, status.Error(codes.InvalidArgument, "missing query")
	}
	if attributeName == "" {
		return nil, status.Error(
			codes.InvalidArgument,
			"missing attribute_name",
		)
	}
	if query.GetStartTimeMin().IsZero() || query.GetStartTimeMax().IsZero() {
		return nil, status.Error(
			codes.InvalidArgument,
			"start_time_min and start_time_max are required",
		)
	}
	if k <= 0 {
		k = defaultAttributeValuesK
	}
	searchDepth := int(query.GetSearchDepth())
	if searchDepth <= 0 {
		searchDepth = defaultAttributeSuggestionSearchDepth
	}

	queryParams := querysvc.TraceQueryParams{
		TraceQueryParams: tracestore.TraceQueryParams{
			ServiceName:   query.GetServiceName(),
			OperationName: query.GetOperationName(),
			Attributes:    jptrace.PlainMapToPcommonMap(query.GetAttributes()),
			SearchDepth:   searchDepth,
			StartTimeMin:  query.GetStartTimeMin(),
			StartTimeMax:  query.GetStartTimeMax(),
			DurationMin:   time.Duration(query.GetDurationMin()),
			DurationMax:   time.Duration(query.GetDurationMax()),
		},
		RawTraces: query.GetRawTraces(),
	}

	findTracesIter := h.QueryService.FindTraces(ctx, queryParams)
	traces, err := jiter.FlattenWithErrors(findTracesIter)
	if err != nil {
		return nil, err
	}
	counts := collectAttributeValueCounts(traces, attributeName)
	return selectKFromCounts(counts, k, desc), nil
}

func receiveTraces(
	seq iter.Seq2[[]ptrace.Traces, error],
	sendFn func(*jptrace.TracesData) error,
) error {
	for traces, err := range seq {
		if err != nil {
			return err
		}
		for _, trace := range traces {
			tracesData := jptrace.TracesData(trace)
			if err := sendFn(&tracesData); err != nil {
				return status.Error(
					codes.Internal,
					fmt.Sprintf(
						"failed to send response stream chunk to client: %v",
						err,
					),
				)
			}
		}
	}
	return nil
}
