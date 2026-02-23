// Copyright (c) 2025 The Jaeger Authors.
// SPDX-License-Identifier: Apache-2.0

package memory

import (
	"context"
	"errors"
	"iter"
	"sort"
	"strconv"
	"strings"
	"sync"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/jaegertracing/jaeger-idl/model/v1"
	"github.com/jaegertracing/jaeger/internal/storage/v2/api/depstore"
	"github.com/jaegertracing/jaeger/internal/storage/v2/api/tracestore"
	conventions "github.com/jaegertracing/jaeger/internal/telemetry/otelsemconv"
	"github.com/jaegertracing/jaeger/internal/tenancy"
)

const errorAttribute = "error"
const internalAttributePrefix = "@jaeger@"

var errInvalidSearchDepth = errors.New("search depth must be greater than 0 and less than max traces")

// Store is an in-memory store of traces
type Store struct {
	mu sync.RWMutex
	// Each tenant gets a copy of default config.
	// In the future this can be extended to contain per-tenant configuration.
	cfg       Configuration
	perTenant map[string]*Tenant
}

// NewStore creates an in-memory store
func NewStore(cfg Configuration) (*Store, error) {
	if cfg.MaxTraces <= 0 {
		return nil, errInvalidMaxTraces
	}
	return &Store{
		cfg:       cfg,
		perTenant: make(map[string]*Tenant),
	}, nil
}

// getTenant returns the per-tenant storage.  Note that tenantID has already been checked for by the collector or query
func (st *Store) getTenant(tenantID string) *Tenant {
	st.mu.RLock()
	tenant, ok := st.perTenant[tenantID]
	st.mu.RUnlock()
	if !ok {
		st.mu.Lock()
		defer st.mu.Unlock()
		tenant, ok = st.perTenant[tenantID]
		if !ok {
			tenant = newTenant(&st.cfg)
			st.perTenant[tenantID] = tenant
		}
	}
	return tenant
}

// WriteTraces write the traces into the tenant by grouping all the spans with same trace id together.
// The traces will not be saved as they are coming, rather they would be reshuffled.
func (st *Store) WriteTraces(ctx context.Context, td ptrace.Traces) error {
	resourceSpansByTraceId := reshuffleResourceSpans(td.ResourceSpans())
	m := st.getTenant(tenancy.GetTenant(ctx))
	m.storeTraces(resourceSpansByTraceId)
	return nil
}

// GetOperations returns operations based on the service name and span kind
func (st *Store) GetOperations(ctx context.Context, query tracestore.OperationQueryParams) ([]tracestore.Operation, error) {
	m := st.getTenant(tenancy.GetTenant(ctx))
	m.mu.RLock()
	defer m.mu.RUnlock()
	var retMe []tracestore.Operation
	if operations, ok := m.operations[query.ServiceName]; ok {
		for operation := range operations {
			if query.SpanKind == "" || query.SpanKind == operation.SpanKind {
				retMe = append(retMe, operation)
			}
		}
	}
	return retMe, nil
}

// GetServices returns a list of all known services
func (st *Store) GetServices(ctx context.Context) ([]string, error) {
	m := st.getTenant(tenancy.GetTenant(ctx))
	m.mu.RLock()
	defer m.mu.RUnlock()
	var retMe []string
	for k := range m.services {
		retMe = append(retMe, k)
	}
	return retMe, nil
}

func (st *Store) GetIndexedAttributesNames(
	ctx context.Context,
	query tracestore.IndexedAttributesNamesQueryParams,
) ([]string, error) {
	if query.Query.Attributes == (pcommon.Map{}) {
		query.Query.Attributes = pcommon.NewMap()
	}
	m := st.getTenant(tenancy.GetTenant(ctx))
	traceAndIDs, err := m.findTraceAndIds(query.Query)
	if err != nil {
		return nil, err
	}
	names := collectIndexedAttributeNames(traceAndIDs)
	sort.Strings(names)
	if query.Limit > 0 && len(names) > query.Limit {
		names = names[:query.Limit]
	}
	return names, nil
}

func (st *Store) GetTopKAttributeValues(
	ctx context.Context,
	query tracestore.KAttributeValuesQueryParams,
) ([]string, error) {
	return st.getKAttributeValues(ctx, query, true)
}

func (st *Store) GetBottomKAttributeValues(
	ctx context.Context,
	query tracestore.KAttributeValuesQueryParams,
) ([]string, error) {
	return st.getKAttributeValues(ctx, query, false)
}

func (st *Store) getKAttributeValues(
	ctx context.Context,
	query tracestore.KAttributeValuesQueryParams,
	desc bool,
) ([]string, error) {
	if query.Query.Attributes == (pcommon.Map{}) {
		query.Query.Attributes = pcommon.NewMap()
	}
	m := st.getTenant(tenancy.GetTenant(ctx))
	traceAndIDs, err := m.findTraceAndIds(query.Query)
	if err != nil {
		return nil, err
	}
	counts := collectAttributeValueCounts(traceAndIDs, query.AttributeName)
	return selectKFromCounts(counts, query.K, desc), nil
}

func (st *Store) FindTraces(ctx context.Context, query tracestore.TraceQueryParams) iter.Seq2[[]ptrace.Traces, error] {
	m := st.getTenant(tenancy.GetTenant(ctx))
	return func(yield func([]ptrace.Traces, error) bool) {
		traceAndIds, err := m.findTraceAndIds(query)
		if err != nil {
			yield(nil, err)
			return
		}
		for i := range traceAndIds {
			if !yield([]ptrace.Traces{traceAndIds[i].trace}, nil) {
				return
			}
		}
	}
}

func (st *Store) FindTraceIDs(ctx context.Context, query tracestore.TraceQueryParams) iter.Seq2[[]tracestore.FoundTraceID, error] {
	m := st.getTenant(tenancy.GetTenant(ctx))
	return func(yield func([]tracestore.FoundTraceID, error) bool) {
		traceAndIds, err := m.findTraceAndIds(query)
		if err != nil {
			yield(nil, err)
			return
		}
		ids := make([]tracestore.FoundTraceID, len(traceAndIds))
		for i := range traceAndIds {
			ids[i] = tracestore.FoundTraceID{TraceID: traceAndIds[i].id}
		}
		yield(ids, nil)
	}
}

func (st *Store) GetTraces(ctx context.Context, traceIDs ...tracestore.GetTraceParams) iter.Seq2[[]ptrace.Traces, error] {
	m := st.getTenant(tenancy.GetTenant(ctx))
	return func(yield func([]ptrace.Traces, error) bool) {
		traces := m.getTraces(traceIDs...)
		for i := range traces {
			if !yield([]ptrace.Traces{traces[i]}, nil) {
				return
			}
		}
	}
}

func (st *Store) GetDependencies(ctx context.Context, query depstore.QueryParameters) ([]model.DependencyLink, error) {
	m := st.getTenant(tenancy.GetTenant(ctx))
	return m.getDependencies(query)
}

func (st *Store) Purge() error {
	st.mu.Lock()
	st.perTenant = make(map[string]*Tenant)
	st.mu.Unlock()
	return nil
}

// reshuffleResourceSpans reshuffles the resource spans so as to group the spans from same traces together. To understand this reshuffling
// take an example of 2 resource spans, then these two resource spans have 2 scope spans each.
// Every scope span consists of 2 spans with trace ids: 1 and 2. Now the final structure should look like:
// For TraceID1: [ResourceSpan1:[ScopeSpan1:[Span(TraceID1)],ScopeSpan2:[Span(TraceID1)], ResourceSpan2:[ScopeSpan1:[Span(TraceID1)],ScopeSpan2:[Span(TraceID1)]
// A similar structure will be there for TraceID2
func reshuffleResourceSpans(resourceSpanSlice ptrace.ResourceSpansSlice) map[pcommon.TraceID]ptrace.ResourceSpansSlice {
	resourceSpansByTraceId := make(map[pcommon.TraceID]ptrace.ResourceSpansSlice)
	for _, resourceSpan := range resourceSpanSlice.All() {
		scopeSpansByTraceId := reshuffleScopeSpans(resourceSpan.ScopeSpans())
		// All the  scope spans here will have the same resource as of resourceSpan. Therefore:
		// Copy the resource to an empty resourceSpan. After this, append the scope spans with same
		// trace id to this empty resource span. Finally move this resource span to the resourceSpanSlice
		// containing other resource spans and having same trace id.
		for traceId, scopeSpansSlice := range scopeSpansByTraceId {
			resourceSpanByTraceId := ptrace.NewResourceSpans()
			resourceSpan.Resource().CopyTo(resourceSpanByTraceId.Resource())
			scopeSpansSlice.MoveAndAppendTo(resourceSpanByTraceId.ScopeSpans())
			resourceSpansSlice, ok := resourceSpansByTraceId[traceId]
			if !ok {
				resourceSpansSlice = ptrace.NewResourceSpansSlice()
				resourceSpansByTraceId[traceId] = resourceSpansSlice
			}
			resourceSpanByTraceId.MoveTo(resourceSpansSlice.AppendEmpty())
		}
	}
	return resourceSpansByTraceId
}

// reshuffleScopeSpans reshuffles all the scope spans of a resource span to group
// spans of same trace ids together. The first step is to iterate the scope spans and then.
// copy the scope to an empty scopeSpan. After this, append the spans with same
// trace id to this empty scope span. Finally move this scope span to the scope span
// slice containing other scope spans and having same trace id.
func reshuffleScopeSpans(scopeSpanSlice ptrace.ScopeSpansSlice) map[pcommon.TraceID]ptrace.ScopeSpansSlice {
	scopeSpansByTraceId := make(map[pcommon.TraceID]ptrace.ScopeSpansSlice)
	for _, scopeSpan := range scopeSpanSlice.All() {
		spansByTraceId := reshuffleSpans(scopeSpan.Spans())
		for traceId, spansSlice := range spansByTraceId {
			scopeSpanByTraceId := ptrace.NewScopeSpans()
			scopeSpan.Scope().CopyTo(scopeSpanByTraceId.Scope())
			spansSlice.MoveAndAppendTo(scopeSpanByTraceId.Spans())
			scopeSpansSlice, ok := scopeSpansByTraceId[traceId]
			if !ok {
				scopeSpansSlice = ptrace.NewScopeSpansSlice()
				scopeSpansByTraceId[traceId] = scopeSpansSlice
			}
			scopeSpanByTraceId.MoveTo(scopeSpansSlice.AppendEmpty())
		}
	}
	return scopeSpansByTraceId
}

func reshuffleSpans(spanSlice ptrace.SpanSlice) map[pcommon.TraceID]ptrace.SpanSlice {
	spansByTraceId := make(map[pcommon.TraceID]ptrace.SpanSlice)
	for _, span := range spanSlice.All() {
		spansSlice, ok := spansByTraceId[span.TraceID()]
		if !ok {
			spansSlice = ptrace.NewSpanSlice()
			spansByTraceId[span.TraceID()] = spansSlice
		}
		span.CopyTo(spansSlice.AppendEmpty())
	}
	return spansByTraceId
}

func getServiceNameFromResource(resource pcommon.Resource) string {
	val, ok := resource.Attributes().Get(conventions.ServiceNameKey)
	if !ok {
		return ""
	}
	return val.Str()
}

func collectIndexedAttributeNames(traceAndIDs []traceAndId) []string {
	set := make(map[string]struct{})
	for i := range traceAndIDs {
		rss := traceAndIDs[i].trace.ResourceSpans()
		for j := 0; j < rss.Len(); j++ {
			rs := rss.At(j)
			rs.Resource().Attributes().Range(func(k string, _ pcommon.Value) bool {
				if strings.HasPrefix(k, internalAttributePrefix) {
					return true
				}
				set[k] = struct{}{}
				return true
			})
			sss := rs.ScopeSpans()
			for k := 0; k < sss.Len(); k++ {
				spans := sss.At(k).Spans()
				for l := 0; l < spans.Len(); l++ {
					spans.At(l).Attributes().Range(func(key string, _ pcommon.Value) bool {
						if strings.HasPrefix(key, internalAttributePrefix) {
							return true
						}
						set[key] = struct{}{}
						return true
					})
				}
			}
		}
	}
	names := make([]string, 0, len(set))
	for k := range set {
		names = append(names, k)
	}
	return names
}

func collectAttributeValueCounts(
	traceAndIDs []traceAndId,
	attributeName string,
) map[string]int {
	counts := make(map[string]int)
	if attributeName == "" || strings.HasPrefix(attributeName, internalAttributePrefix) {
		return counts
	}
	for i := range traceAndIDs {
		rss := traceAndIDs[i].trace.ResourceSpans()
		for j := 0; j < rss.Len(); j++ {
			rs := rss.At(j)
			incrementCountsForAttribute(rs.Resource().Attributes(), attributeName, counts)
			sss := rs.ScopeSpans()
			for k := 0; k < sss.Len(); k++ {
				spans := sss.At(k).Spans()
				for l := 0; l < spans.Len(); l++ {
					incrementCountsForAttribute(spans.At(l).Attributes(), attributeName, counts)
				}
			}
		}
	}
	return counts
}

func incrementCountsForAttribute(attrs pcommon.Map, attributeName string, counts map[string]int) {
	v, ok := attrs.Get(attributeName)
	if !ok {
		return
	}
	value, ok := attributeValueToString(v)
	if !ok {
		return
	}
	counts[value]++
}

func attributeValueToString(v pcommon.Value) (string, bool) {
	switch v.Type() {
	case pcommon.ValueTypeStr:
		if v.Str() == "" {
			return "", false
		}
		return v.Str(), true
	case pcommon.ValueTypeBool:
		return strconv.FormatBool(v.Bool()), true
	case pcommon.ValueTypeInt:
		return strconv.FormatInt(v.Int(), 10), true
	case pcommon.ValueTypeDouble:
		return strconv.FormatFloat(v.Double(), 'f', -1, 64), true
	default:
		return "", false
	}
}

func selectKFromCounts(counts map[string]int, k int, desc bool) []string {
	type entry struct {
		value string
		count int
	}
	entries := make([]entry, 0, len(counts))
	for value, count := range counts {
		entries = append(entries, entry{value: value, count: count})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].count == entries[j].count {
			return entries[i].value < entries[j].value
		}
		if desc {
			return entries[i].count > entries[j].count
		}
		return entries[i].count < entries[j].count
	})
	if k <= 0 {
		return nil
	}
	if len(entries) > k {
		entries = entries[:k]
	}
	values := make([]string, len(entries))
	for i := range entries {
		values[i] = entries[i].value
	}
	return values
}
