// Copyright (c) 2026 The Jaeger Authors.
// SPDX-License-Identifier: Apache-2.0

package tracestore

import (
	"context"
	"errors"
)

var ErrIndexedAttributesNamesNotSupported = errors.New("indexed attributes names query is not supported by this storage backend")

// IndexedAttributesNamesQueryParams contains parameters for querying indexed
// attribute names from storage.
type IndexedAttributesNamesQueryParams struct {
	Query TraceQueryParams
	Limit int
}

// IndexedAttributesReader can be implemented by storage readers that can
// directly query indexed attribute names.
type IndexedAttributesReader interface {
	GetIndexedAttributesNames(context.Context, IndexedAttributesNamesQueryParams) ([]string, error)
}
