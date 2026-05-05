package grpcserver

import (
	"fmt"
	"net"

	paymentpb "github.com/RoyalTejShinde1993/distributed-payments-platform/pkg/api/payment/v1"
	"google.golang.org/grpc"
)

type Server struct {
	grpcServer *grpc.Server
}

func NewServer() *Server {
	return &Server{
		grpcServer: grpc.NewServer(),
	}
}

func (s *Server) RegisterService(service paymentpb.PaymentServiceServer) {
	paymentpb.RegisterPaymentServiceServer(s.grpcServer, service)
}

func (s *Server) Start(address string) error {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("listen failed: %w", err)
	}

	go func() {
		if serveErr := s.grpcServer.Serve(listener); serveErr != nil {
			fmt.Printf("grpc server error: %v\n", serveErr)
		}
	}()

	return nil
}

func (s *Server) Stop() {
	s.grpcServer.GracefulStop()
}
