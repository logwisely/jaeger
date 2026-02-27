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
