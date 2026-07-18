# 🚀 Real-Time Crypto Price Tracker (gRPC Demo)

A lightweight, high-performance microservice application built in Go using gRPC. This project demonstrates **Server-Side Streaming**, where the server continuously pushes simulated cryptocurrency price updates to a client over a single HTTP/2 connection.

## 🛠️ Project Structure
```text
grpc-crypto-demo/
├── README.md
├── go.mod
├── go.sum
├── pb/
│   ├── crypto.pb.go
│   └── crypto_grpc.pb.go
├── proto/
│   └── crypto.proto
├── client/
│   └── main.go
└── server/
    └── main.go
```

---

## 📋 Step 1: The Protocol Buffer Definition
Create a file named `proto/crypto.proto`. This acts as the API contract between the server and the client.

```protobuf
syntax = "proto3";

package crypto;

option go_package = "./pb";

// The PriceService definition.
service PriceService {
  // Server-side streaming RPC to get real-time price updates
  rpc StreamPrices (PriceRequest) returns (stream PriceResponse);
}

// Request payload containing the requested coin tickers
message PriceRequest {
  repeated string tickers = 1; // e.g., ["BTC", "ETH"]
}

// Response payload containing individual price ticks
message PriceResponse {
  string ticker = 1;
  double price = 2;
  int64 timestamp = 3;
}
```

---

## 🛠️ Step 2: Initialize Go Module & Install Dependencies
Run these commands in your terminal to set up the Go project and install the necessary gRPC tools.

```bash
# Initialize the module
go mod init grpc-crypto-demo

# Install gRPC and Protobuf modules for Go
go get google.golang.org/grpc
go get google.golang.org/protobuf/cmd/protoc-gen-go
go get google.golang.org/grpc/cmd/protoc-gen-go-grpc

# Compile the proto file (Ensure you have protoc installed on your OS)
protoc --go_out=. --go-grpc_out=. proto/crypto.proto
```

---

## 🖥️ Step 3: Implement the Server
Create a file named `server/main.go`. This server implements the `PriceService` and streams mock price data.

```go
package main

import (
	"log"
	"math/rand"
	"net"
	"time"

	"grpc-crypto-demo/pb"

	"google.golang.org/grpc"
)

type server struct {
	pb.UnimplementedPriceServiceServer
}

func (s *server) StreamPrices(req *pb.PriceRequest, stream pb.PriceService_StreamPricesServer) error {
	log.Printf("Received streaming request for tickers: %v", req.Tickers)

	// Base prices for simulation
	prices := map[string]float64{
		"BTC": 65000.0,
		"ETH": 3500.0,
		"SOL": 140.0,
	}

	for {
		for _, ticker := range req.Tickers {
			basePrice, exists := prices[ticker]
			if !exists {
				basePrice = 10.0 // Default for unknown tickers
			}

			// Simulate minor price fluctuation (-0.5% to +0.5%)
			change := (rand.Float64() - 0.5) * 0.01 * basePrice
			prices[ticker] = basePrice + change

			// Create response payload
			res := &pb.PriceResponse{
				Ticker:    ticker,
				Price:     prices[ticker],
				Timestamp: time.Now().Unix(),
			}

			// Send the message over the gRPC stream
			if err := stream.Send(res); err != nil {
				log.Printf("Failed to send price tick for %s: %v", ticker, err)
				return err
			}
		}
		// Wait 1 second before sending the next batch of updates
		time.Sleep(1 * time.Second)
	}
}

func main() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen on port 50051: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterPriceServiceServer(s, &server{})

	log.Println("gRPC Server running locally on port :50051...")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve gRPC: %v", err)
	}
}
```

---

## 🔌 Step 4: Implement the Client
Create a file named `client/main.go`. This client connects to the server and listens to the price stream.

```go
package main

import (
	"context"
	"io"
	"log"

	"grpc-crypto-demo/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// Set up connection to the server
	conn, err := grpc.Dial("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Did not connect to server: %v", err)
	}
	defer conn.Close()

	client := pb.NewPriceServiceClient(conn)

	// Define which tickers we want to track
	req := &pb.PriceRequest{
		Tickers: []string{"BTC", "ETH", "SOL"},
	}

	// Call the streaming RPC method
	stream, err := client.StreamPrices(context.Background(), req)
	if err != nil {
		log.Fatalf("Error calling StreamPrices: %v", err)
	}

	log.Println("Connected to server. Listening for price updates...")

	// Receive streaming responses continuously
	for {
		res, err := stream.Recv()
		if err == io.EOF {
			log.Println("Stream closed by server.")
			break
		}
		if err != nil {
			log.Fatalf("Error while receiving stream: %v", err)
		}

		log.Printf("📊 [TICKER] %s | 💸 [PRICE] $%.2f | 🕒 [TS] %d", res.Ticker, res.Price, res.Timestamp)
	}
}
```

---

## 🏃 Step 5: How to Run the App

1. **Start the gRPC Server**:
   ```bash
   go run server/main.go
   ```

2. **Start the gRPC Client** (in a separate terminal window):
   ```bash
   go run client/main.go
   ```

You will see the client terminal update every second with simulated live prices for BTC, ETH, and SOL!
