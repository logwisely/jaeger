// Copyright (c) 2026 The Jaeger Authors.
// SPDX-License-Identifier: Apache-2.0

package tracestore

import (
	"context"
	"errors"
)

var ErrAttributeValuesQueryNotSupported = errors.New("attribute values query is not supported by this storage backend")

// KAttributeValuesQueryParams contains parameters for querying ranked attribute
// values from storage.
type KAttributeValuesQueryParams struct {
	Query         TraceQueryParams
	AttributeName string
	K             int
}

// AttributeValuesReader can be implemented by storage readers that can query
// ranked attribute values directly.
type AttributeValuesReader interface {
	GetTopKAttributeValues(context.Context, KAttributeValuesQueryParams) ([]string, error)
	GetBottomKAttributeValues(context.Context, KAttributeValuesQueryParams) ([]string, error)
}
