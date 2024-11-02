package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/deltalake/protos"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"io"
	"log"
	"time"
)

const (
	defaultName = "world"
)

var (
	addr = flag.String("addr", "localhost:9000", "the address to connect to")
	name = flag.String("name", defaultName, "Name to greet")
)

func main() {
	flag.Parse()
	// Set up a connection to the server.
	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := protos.NewReaderServiceClient(conn)

	// Contact the server and print out its response.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	r, err := c.Scan(ctx, &protos.GetRequest{Table: "test"})
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}

	for {
		resp, err := r.Recv()
		if err == io.EOF {
			return
		} else if err == nil {
			valStr := fmt.Sprintf("Response: %v", resp.GetData())
			log.Println(valStr)
		}

		if err != nil {
			panic(err)
		}
	}
}
