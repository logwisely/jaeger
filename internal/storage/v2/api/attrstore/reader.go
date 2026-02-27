// Copyright (c) 2026 The Jaeger Authors.
// SPDX-License-Identifier: Apache-2.0

package attrstore

import (
	"context"
	"errors"
	"time"
)

// ErrNotSupported indicates that the storage backend does not support
// the requested attributes query.
var ErrNotSupported = errors.New("attributes query is not supported")

// GetIndexedAttributesNamesParams defines the query parameters for
// retrieving indexed attribute names.
type GetIndexedAttributesNamesParams struct {
	WorkspaceID   string
	ServiceName   string
	OperationName string
	StartTimeMin  time.Time
	StartTimeMax  time.Time
	Limit         int
}

// Reader provides access to attributes metadata for querying.
type Reader interface {
	// GetIndexedAttributesNames returns a list of "indexed" attribute names that
	// the backend can efficiently search/filter on.
	GetIndexedAttributesNames(ctx context.Context, params GetIndexedAttributesNamesParams) ([]string, error)
}
