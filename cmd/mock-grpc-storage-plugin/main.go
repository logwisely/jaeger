// Copyright (c) 2026 The Jaeger Authors.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	storagev2 "github.com/jaegertracing/jaeger/internal/proto-gen/storage/v2"
)

type mockTraceReader struct {
	storagev2.UnimplementedTraceReaderServer
	storagev2.UnimplementedAttributesReaderServer
}

func (mockTraceReader) GetIndexedAttributesNames(
	_ context.Context,
	_ *storagev2.GetIndexedAttributesNamesRequest,
) (*storagev2.GetAttributesNamesResponse, error) {
	return &storagev2.GetAttributesNamesResponse{
		Names: []string{
			"http.method",
			"http.route",
			"http.status_code",
			"db.system",
			"rpc.system",
		},
	}, nil
}

func main() {
	listenAddr := flag.String("listen", ":17271", "gRPC listen address")
	flag.Parse()

	var lc net.ListenConfig
	lis, err := lc.Listen(context.Background(), "tcp", *listenAddr)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", *listenAddr, err)
	}

	grpcServer := grpc.NewServer()
	hs := health.NewServer()
	mock := &mockTraceReader{}
	storagev2.RegisterTraceReaderServer(grpcServer, mock)
	storagev2.RegisterAttributesReaderServer(grpcServer, mock)
	grpc_health_v1.RegisterHealthServer(grpcServer, hs)
	reflection.Register(grpcServer)

	hs.SetServingStatus("jaeger.storage.v2.TraceReader", grpc_health_v1.HealthCheckResponse_SERVING)
	hs.SetServingStatus("jaeger.storage.v2.AttributesReader", grpc_health_v1.HealthCheckResponse_SERVING)

	go func() {
		log.Printf("mock gRPC storage plugin listening on %s", lis.Addr().String())
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("gRPC server exited: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	grpcServer.GracefulStop()
}
