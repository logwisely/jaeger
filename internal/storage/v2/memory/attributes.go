// Copyright (c) 2026 The Jaeger Authors.
// SPDX-License-Identifier: Apache-2.0

package memory

import (
	"context"
	"sort"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/jaegertracing/jaeger/internal/storage/v2/api/attrstore"
	"github.com/jaegertracing/jaeger/internal/tenancy"
)

var _ attrstore.Reader = (*Store)(nil)

const maxTracesForTopK = 100

func (st *Store) GetTopKAttributeValues(
	ctx context.Context,
	params attrstore.GetTopKAttributeValuesParams,
) ([]string, error) {
	if params.AttributeName == "" || params.K <= 0 {
		return []string{}, nil
	}

	tenant := st.getTenant(tenancy.GetTenant(ctx))
	tenant.mu.RLock()
	defer tenant.mu.RUnlock()

	if tenant.mostRecent < 0 {
		return []string{}, nil
	}

	valueCounts := make(map[string]int)
	n := len(tenant.traces)
	matchedTraces := 0
	for i := 0; i < n && matchedTraces < maxTracesForTopK; i++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		index := (tenant.mostRecent - i + n) % n
		traceByID := tenant.traces[index]
		if traceByID.id.IsEmpty() {
			break
		}
		if collectTopKAttributeValuesFromTrace(traceByID.trace, params, valueCounts) {
			matchedTraces++
		}
	}
	if len(valueCounts) == 0 {
		return []string{}, nil
	}

	type valueAndCount struct {
		value string
		count int
	}
	ranked := make([]valueAndCount, 0, len(valueCounts))
	for value, count := range valueCounts {
		ranked = append(ranked, valueAndCount{value: value, count: count})
	}
	sort.Slice(ranked, func(i, j int) bool {
		if ranked[i].count != ranked[j].count {
			return ranked[i].count > ranked[j].count
		}
		return ranked[i].value < ranked[j].value
	})

	limit := params.K
	if limit > len(ranked) {
		limit = len(ranked)
	}
	values := make([]string, 0, limit)
	for i := 0; i < limit; i++ {
		values = append(values, ranked[i].value)
	}
	return values, nil
}

func (st *Store) GetIndexedAttributesNames(ctx context.Context, params attrstore.GetIndexedAttributesNamesParams) ([]string, error) {
	tenant := st.getTenant(tenancy.GetTenant(ctx))
	tenant.mu.RLock()
	defer tenant.mu.RUnlock()

	namesSet := make(map[string]struct{})
	for _, traceIndex := range tenant.ids {
		collectIndexedAttributeNamesFromTrace(tenant.traces[traceIndex].trace, params, namesSet)
	}

	if len(namesSet) == 0 {
		return []string{}, nil
	}

	names := make([]string, 0, len(namesSet))
	for name := range namesSet {
		names = append(names, name)
	}
	sort.Strings(names)

	if params.Limit > 0 && len(names) > params.Limit {
		names = names[:params.Limit]
	}
	return names, nil
}

func collectIndexedAttributeNamesFromTrace(trace ptrace.Traces, params attrstore.GetIndexedAttributesNamesParams, namesSet map[string]struct{}) {
	for _, resourceSpan := range trace.ResourceSpans().All() {
		serviceName := getServiceNameFromResource(resourceSpan.Resource())
		if params.ServiceName != "" && params.ServiceName != serviceName {
			continue
		}

		resourceAttrsAdded := false
		for _, scopeSpan := range resourceSpan.ScopeSpans().All() {
			for _, span := range scopeSpan.Spans().All() {
				if !spanMatchesAttributesQuery(span, params) {
					continue
				}

				if !resourceAttrsAdded {
					addAttributeKeys(resourceSpan.Resource().Attributes(), namesSet)
					resourceAttrsAdded = true
				}
				addAttributeKeys(scopeSpan.Scope().Attributes(), namesSet)
				addAttributeKeys(span.Attributes(), namesSet)
				for _, event := range span.Events().All() {
					addAttributeKeys(event.Attributes(), namesSet)
				}
				for _, link := range span.Links().All() {
					addAttributeKeys(link.Attributes(), namesSet)
				}
			}
		}
	}
}

func addAttributeKeys(attrs pcommon.Map, namesSet map[string]struct{}) {
	attrs.Range(func(k string, _ pcommon.Value) bool {
		namesSet[k] = struct{}{}
		return true
	})
}

func collectTopKAttributeValuesFromTrace(
	trace ptrace.Traces,
	params attrstore.GetTopKAttributeValuesParams,
	valueCounts map[string]int,
) bool {
	traceMatched := false
	for _, resourceSpan := range trace.ResourceSpans().All() {
		serviceName := getServiceNameFromResource(resourceSpan.Resource())
		if params.ServiceName != "" && params.ServiceName != serviceName {
			continue
		}
		resourceAttrsCounted := false
		for _, scopeSpan := range resourceSpan.ScopeSpans().All() {
			scopeAttrsCounted := false
			for _, span := range scopeSpan.Spans().All() {
				if !spanMatchesTopKQuery(span, params) {
					continue
				}
				traceMatched = true
				if !resourceAttrsCounted {
					addStringAttributeValue(resourceSpan.Resource().Attributes(), params.AttributeName, valueCounts)
					resourceAttrsCounted = true
				}
				if !scopeAttrsCounted {
					addStringAttributeValue(scopeSpan.Scope().Attributes(), params.AttributeName, valueCounts)
					scopeAttrsCounted = true
				}
				addStringAttributeValue(span.Attributes(), params.AttributeName, valueCounts)
				for _, event := range span.Events().All() {
					addStringAttributeValue(event.Attributes(), params.AttributeName, valueCounts)
				}
				for _, link := range span.Links().All() {
					addStringAttributeValue(link.Attributes(), params.AttributeName, valueCounts)
				}
			}
		}
	}
	return traceMatched
}

func addStringAttributeValue(attrs pcommon.Map, key string, valueCounts map[string]int) {
	val, ok := attrs.Get(key)
	if !ok || val.Type() != pcommon.ValueTypeStr {
		return
	}
	valueCounts[val.Str()]++
}

func spanMatchesTopKQuery(span ptrace.Span, params attrstore.GetTopKAttributeValuesParams) bool {
	if params.OperationName != "" && params.OperationName != span.Name() {
		return false
	}

	startTime := span.StartTimestamp().AsTime()
	if !params.StartTimeMin.IsZero() && startTime.Before(params.StartTimeMin) {
		return false
	}
	if !params.StartTimeMax.IsZero() && startTime.After(params.StartTimeMax) {
		return false
	}
	return true
}

func spanMatchesAttributesQuery(span ptrace.Span, params attrstore.GetIndexedAttributesNamesParams) bool {
	if params.OperationName != "" && params.OperationName != span.Name() {
		return false
	}

	startTime := span.StartTimestamp().AsTime()
	if !params.StartTimeMin.IsZero() && startTime.Before(params.StartTimeMin) {
		return false
	}
	if !params.StartTimeMax.IsZero() && startTime.After(params.StartTimeMax) {
		return false
	}
	return true
}
