package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	protos2 "github.com/deltalake/protos"
	"log"
	"log/slog"
	"net"

	"github.com/deltalake"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type Server struct {
	protos2.UnimplementedReaderServiceServer
	protos2.UnimplementedWriterServiceServer

	delta      deltalake.DeltaStorage
	objStorage deltalake.ObjectStorage

	txs map[int64]*deltalake.Transaction
}

var _ protos2.WriterServiceServer = (*Server)(nil)
var _ protos2.ReaderServiceServer = (*Server)(nil)

func (s *Server) Scan(in *protos2.GetRequest, response grpc.ServerStreamingServer[protos2.DataResponse]) (err error) {
	log.Printf("Received request to scan table: %s", in.Table)
	table := in.Table

	tx, _ := s.getTx(in.TxId)
	it, err := tx.Iter(table)
	if err != nil {
		return err
	}

	// todo: ugly architecture
	mut := func(xs []any) []string {
		slog.Debug("got following values", slog.Any("data", xs))
		res := make([]string, len(xs))
		for i, v := range xs {
			res[i] = fmt.Sprintf("%v", v)
		}
		return res
	}
	var (
		v []any
	)
	for v, err = it.First(); err == nil; v, err = it.Next() {
		if err = response.Send(&protos2.DataResponse{
			Data: mut(v),
		}); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) Set(ctx context.Context, in *protos2.SetRequest) (*protos2.Error, error) {
	// ugly
	table, values := in.Table, in.Values
	tx, has := s.getTx(in.TxId)
	input := func() []any {
		res := make([]any, len(values))
		for i, val := range values {
			res[i] = any(val)
		}
		return res
	}()
	if err := tx.Put(table, input); err != nil {
		return &protos2.Error{
			Status:  500,
			Message: err.Error(),
		}, err
	}
	if !has {
		if err := tx.Commit(); err != nil {
			return &protos2.Error{
				Status:  500,
				Message: err.Error(),
			}, err
		}
	}
	return &protos2.Error{
		Status: 200,
	}, nil
}

func (s *Server) Create(ctx context.Context, in *protos2.CreateRequest) (*protos2.Error, error) {
	tx, has := s.getTx(in.TxId)
	table := in.Table
	if err := tx.Create(table, in.Columns); err != nil {
		return &protos2.Error{
			Status:  500,
			Message: err.Error(),
		}, err
	}

	if !has {
		if err := tx.Commit(); err != nil {
			return &protos2.Error{
				Status:  500,
				Message: err.Error(),
			}, err
		}
	}

	return &protos2.Error{
		Status: 200,
	}, nil
}

func (s *Server) NewTransaction(context.Context, *protos2.Empty) (*protos2.Transaction, error) {
	tx := s.delta.NewTransaction()
	s.txs[tx.GetId()] = tx
	return &protos2.Transaction{TxId: tx.GetId()}, nil
}

func (s *Server) Commit(ctx context.Context, in *protos2.Transaction) (*protos2.Error, error) {
	tx, ok := s.txs[in.TxId]
	if !ok {
		return &protos2.Error{Status: 500, Message: "invalid transaction id"}, errors.New("invalid transaction id")
	}

	err := tx.Commit()
	if err != nil {
		return &protos2.Error{Status: 500, Message: err.Error()}, err
	}
	delete(s.txs, in.TxId)
	return &protos2.Error{Status: 200}, nil
}

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
		txs:        make(map[int64]*deltalake.Transaction),
	}

	grpcServer := grpc.NewServer()
	protos2.RegisterReaderServiceServer(grpcServer, &s)
	protos2.RegisterWriterServiceServer(grpcServer, &s)
	reflection.Register(grpcServer)

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %s", err)
	}
}

func (s *Server) getTx(id *int64) (*deltalake.Transaction, bool) {
	if id == nil {
		return s.delta.NewTransaction(), false
	}
	if tx, ok := s.txs[*id]; ok {
		return tx, true
	}
	return s.delta.NewTransaction(), false
}
