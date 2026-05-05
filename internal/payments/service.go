package payments

import (
	"context"
	"log"
	"sync"
	"time"

	grpcserver "github.com/RoyalTejShinde1993/distributed-payments-platform/internal/payments/transport/grpc"
	"github.com/RoyalTejShinde1993/distributed-payments-platform/internal/payments/transport/kafka"
	paymentpb "github.com/RoyalTejShinde1993/distributed-payments-platform/pkg/api/payment/v1"
	"google.golang.org/protobuf/proto"
)

type Config struct {
	GRPCAddress   string
	KafkaBrokers  []string
	PaymentTopic  string
	ConsumerGroup string
}

type Service struct {
	cfg        Config
	producer   *kafka.Producer
	consumer   *kafka.Consumer
	grpcServer *grpcserver.Server
	wg         sync.WaitGroup
}

func NewService(cfg Config) *Service {
	return &Service{cfg: cfg}
}

func (s *Service) Start(ctx context.Context) error {
	s.producer = kafka.NewProducer(s.cfg.KafkaBrokers, s.cfg.PaymentTopic)
	s.consumer = kafka.NewConsumer(s.cfg.KafkaBrokers, s.cfg.ConsumerGroup, s.cfg.PaymentTopic)
	s.grpcServer = grpcserver.NewServer()

	handler := grpcserver.NewHandler(s.producer)
	s.grpcServer.RegisterService(handler)

	if err := s.grpcServer.Start(s.cfg.GRPCAddress); err != nil {
		return err
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if err := s.consumer.Start(ctx, s.handleEvent); err != nil {
			log.Printf("kafka consumer stopped: %v", err)
		}
	}()

	return nil
}

func (s *Service) Stop() {
	s.grpcServer.Stop()
	if s.consumer != nil {
		_ = s.consumer.Close()
	}
	if s.producer != nil {
		_ = s.producer.Close()
	}
	s.wg.Wait()
}

func (s *Service) ProcessPayment(ctx context.Context, req *paymentpb.PaymentRequest) (*paymentpb.PaymentResponse, error) {
	event := &paymentpb.PaymentEvent{
		Id:        req.Id,
		Amount:    req.Amount,
		Currency:  req.Currency,
		Status:    "PENDING",
		CreatedAt: time.Now().Unix(),
	}

	payload, err := proto.Marshal(event)
	if err != nil {
		return nil, err
	}

	if err := s.producer.Publish(ctx, []byte(req.Id), payload); err != nil {
		return nil, err
	}

	return &paymentpb.PaymentResponse{
		Id:      req.Id,
		Status:  "ACCEPTED",
		Message: "payment submitted",
	}, nil
}

func (s *Service) handleEvent(message []byte) error {
	log.Printf("payment event received: %s", string(message))
	return nil
}
