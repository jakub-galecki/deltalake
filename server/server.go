package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"

	"github.com/deltalake"
	"github.com/deltalake/server/protos"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type Server struct {
	protos.UnimplementedReaderServiceServer
	protos.UnimplementedWriterServiceServer

	delta      deltalake.DeltaStorage
	objStorage deltalake.ObjectStorage
}

func (s *Server) Scan(in *protos.GetRequest, response grpc.ServerStreamingServer[protos.DataResponse]) error {
	log.Printf("Received request to scan table: %s", in.Table)
	return nil
}

func (s *Server) Set(ctx context.Context, in *protos.SetRequest) (*protos.Error, error) {
	// ugly
	table, values := in.Table, in.Values
	tx := s.delta.NewTransaction()
	input := func() []any {
		res := make([]any, len(values))
		for i, val := range values {
			res[i] = any(val)
		}
		return res
	}()
	if err := tx.Put(table, input); err != nil {
		return &protos.Error{
			Status:  500,
			Message: err.Error(),
		}, err
	}
	if err := tx.Commit(); err != nil {
		return &protos.Error{
			Status:  500,
			Message: err.Error(),
		}, err
	}
	return &protos.Error{
		Status: 200,
	}, nil
}

func (s *Server) Create(ctx context.Context, in *protos.CreateRequest) (*protos.Error, error) {
	table := in.Table
	tx := s.delta.NewTransaction()
	if err := tx.Create(table, in.Colums); err != nil {
		return &protos.Error{
			Status:  500,
			Message: err.Error(),
		}, err
	}
	if err := tx.Commit(); err != nil {
		return &protos.Error{
			Status:  500,
			Message: err.Error(),
		}, err
	}
	return &protos.Error{
		Status: 200,
	}, nil
}

// todo: add id to metada and store transactions
func main() {
	storageType := flag.Int("storage", 0, "storage type: 0 - local")
	storageDst := flag.String("storageDst", "", "where storage should be kept")
	flag.Parse()

	if storageType == nil || *storageType != 0 {
		panic("invalid storage type")
	}

	if storageDst == nil {
		panic("no storage destination provided")
	}

	store := deltalake.NewFileStorage(*storageDst)
	d := deltalake.New(store, deltalake.DefaultOpts())

	log.Printf("Starting server on port 9000")
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", 9000))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := Server{
		delta:      d,
		objStorage: store,
	}

	grpcServer := grpc.NewServer()
	protos.RegisterReaderServiceServer(grpcServer, &s)
	protos.RegisterWriterServiceServer(grpcServer, &s)
	reflection.Register(grpcServer)

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %s", err)
	}
}
