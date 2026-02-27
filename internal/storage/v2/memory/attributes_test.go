// Copyright (c) 2026 The Jaeger Authors.
// SPDX-License-Identifier: Apache-2.0

package memory

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jaegertracing/jaeger/internal/storage/v2/api/attrstore"
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
