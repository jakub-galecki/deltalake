// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.5.1
// - protoc             v3.21.8
// source: protos/writer.proto

package protos

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.64.0 or later.
const _ = grpc.SupportPackageIsVersion9

const (
	WriterService_Create_FullMethodName = "/protos.WriterService/Create"
	WriterService_Set_FullMethodName    = "/protos.WriterService/Set"
)

// WriterServiceClient is the client API for WriterService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type WriterServiceClient interface {
	Create(ctx context.Context, in *CreateRequest, opts ...grpc.CallOption) (*Error, error)
	Set(ctx context.Context, in *SetRequest, opts ...grpc.CallOption) (*Error, error)
}

type writerServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewWriterServiceClient(cc grpc.ClientConnInterface) WriterServiceClient {
	return &writerServiceClient{cc}
}

func (c *writerServiceClient) Create(ctx context.Context, in *CreateRequest, opts ...grpc.CallOption) (*Error, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(Error)
	err := c.cc.Invoke(ctx, WriterService_Create_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *writerServiceClient) Set(ctx context.Context, in *SetRequest, opts ...grpc.CallOption) (*Error, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(Error)
	err := c.cc.Invoke(ctx, WriterService_Set_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// WriterServiceServer is the server API for WriterService service.
// All implementations must embed UnimplementedWriterServiceServer
// for forward compatibility.
type WriterServiceServer interface {
	Create(context.Context, *CreateRequest) (*Error, error)
	Set(context.Context, *SetRequest) (*Error, error)
	mustEmbedUnimplementedWriterServiceServer()
}

// UnimplementedWriterServiceServer must be embedded to have
// forward compatible implementations.
//
// NOTE: this should be embedded by value instead of pointer to avoid a nil
// pointer dereference when methods are called.
type UnimplementedWriterServiceServer struct{}

func (UnimplementedWriterServiceServer) Create(context.Context, *CreateRequest) (*Error, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Create not implemented")
}
func (UnimplementedWriterServiceServer) Set(context.Context, *SetRequest) (*Error, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Set not implemented")
}
func (UnimplementedWriterServiceServer) mustEmbedUnimplementedWriterServiceServer() {}
func (UnimplementedWriterServiceServer) testEmbeddedByValue()                       {}

// UnsafeWriterServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to WriterServiceServer will
// result in compilation errors.
type UnsafeWriterServiceServer interface {
	mustEmbedUnimplementedWriterServiceServer()
}

func RegisterWriterServiceServer(s grpc.ServiceRegistrar, srv WriterServiceServer) {
	// If the following call pancis, it indicates UnimplementedWriterServiceServer was
	// embedded by pointer and is nil.  This will cause panics if an
	// unimplemented method is ever invoked, so we test this at initialization
	// time to prevent it from happening at runtime later due to I/O.
	if t, ok := srv.(interface{ testEmbeddedByValue() }); ok {
		t.testEmbeddedByValue()
	}
	s.RegisterService(&WriterService_ServiceDesc, srv)
}

func _WriterService_Create_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CreateRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(WriterServiceServer).Create(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: WriterService_Create_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(WriterServiceServer).Create(ctx, req.(*CreateRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _WriterService_Set_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(SetRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(WriterServiceServer).Set(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: WriterService_Set_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(WriterServiceServer).Set(ctx, req.(*SetRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// WriterService_ServiceDesc is the grpc.ServiceDesc for WriterService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var WriterService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "protos.WriterService",
	HandlerType: (*WriterServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Create",
			Handler:    _WriterService_Create_Handler,
		},
		{
			MethodName: "Set",
			Handler:    _WriterService_Set_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "protos/writer.proto",
}