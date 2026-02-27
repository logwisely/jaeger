// Copyright (c) 2026 The Jaeger Authors.
// SPDX-License-Identifier: Apache-2.0

package grpc

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/jaegertracing/jaeger/internal/proto-gen/storage/v2"
	"github.com/jaegertracing/jaeger/internal/storage/v2/api/attrstore"
)

var _ attrstore.Reader = (*AttributesReader)(nil)

type AttributesReader struct {
	client storage.AttributesReaderClient
}

// NewAttributesReader creates an AttributesReader that communicates with a remote gRPC storage server.
func NewAttributesReader(conn *grpc.ClientConn) *AttributesReader {
	return &AttributesReader{client: storage.NewAttributesReaderClient(conn)}
}

func (ar *AttributesReader) GetIndexedAttributesNames(
	ctx context.Context,
	params attrstore.GetIndexedAttributesNamesParams,
) ([]string, error) {
	limit := int32(params.Limit) //nolint:gosec // G115
	req := &storage.GetIndexedAttributesNamesRequest{
		WorkspaceId:   params.WorkspaceID,
		ServiceName:   params.ServiceName,
		OperationName: params.OperationName,
		Query: &storage.AttributesQueryParameters{
			WorkspaceId:   params.WorkspaceID,
			ServiceName:   params.ServiceName,
			OperationName: params.OperationName,
			StartTimeMin:  params.StartTimeMin,
			StartTimeMax:  params.StartTimeMax,
			Limit:         limit,
		},
	}

	resp, err := ar.client.GetIndexedAttributesNames(ctx, req)
	if err != nil {
		if status.Code(err) == codes.Unimplemented {
			return nil, attrstore.ErrNotSupported
		}
		// Some gRPC servers may not register the AttributesReader service at all.
		// In that case gRPC also returns codes.Unimplemented, which is handled above.
		return nil, fmt.Errorf("failed to execute GetIndexedAttributesNames: %w", err)
	}
	if resp == nil {
		return []string{}, nil
	}
	// Defensive: treat nil slice as empty.
	if resp.Names == nil {
		return []string{}, nil
	}
	// Ensure we don't leak internal slice for callers that may append.
	names := make([]string, len(resp.Names))
	copy(names, resp.Names)
	return names, nil
}
