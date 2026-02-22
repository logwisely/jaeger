// Copyright (c) 2026 The Jaeger Authors.
// SPDX-License-Identifier: Apache-2.0

package apiv3

import (
	"sort"
	"strconv"
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

const (
	defaultAttributeSuggestionSearchDepth = 100
	defaultAttributeNamesLimit            = 100
	defaultAttributeValuesK               = 10
)

func collectAttributeNames(traces []ptrace.Traces) []string {
	set := make(map[string]struct{})
	for _, tr := range traces {
		rss := tr.ResourceSpans()
		for i := 0; i < rss.Len(); i++ {
			rs := rss.At(i)
			rs.Resource().Attributes().Range(func(k string, _ pcommon.Value) bool {
				if isInternalAttributeKey(k) {
					return true
				}
				set[k] = struct{}{}
				return true
			})
			sss := rs.ScopeSpans()
			for j := 0; j < sss.Len(); j++ {
				spans := sss.At(j).Spans()
				for k := 0; k < spans.Len(); k++ {
					spans.At(k).Attributes().Range(func(key string, _ pcommon.Value) bool {
						if isInternalAttributeKey(key) {
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

func collectAttributeValueCounts(traces []ptrace.Traces, attributeName string) map[string]int {
	counts := make(map[string]int)
	if attributeName == "" {
		return counts
	}
	if isInternalAttributeKey(attributeName) {
		return counts
	}
	for _, tr := range traces {
		rss := tr.ResourceSpans()
		for i := 0; i < rss.Len(); i++ {
			rs := rss.At(i)
			incrementCountsForAttribute(rs.Resource().Attributes(), attributeName, counts)
			sss := rs.ScopeSpans()
			for j := 0; j < sss.Len(); j++ {
				spans := sss.At(j).Spans()
				for k := 0; k < spans.Len(); k++ {
					incrementCountsForAttribute(spans.At(k).Attributes(), attributeName, counts)
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

func isInternalAttributeKey(k string) bool {
	return strings.HasPrefix(k, "@jaeger@")
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
