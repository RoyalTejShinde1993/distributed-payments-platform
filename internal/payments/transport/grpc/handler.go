package grpcserver

import (
	"context"
	"io"
	"time"

	"github.com/RoyalTejShinde1993/distributed-payments-platform/internal/payments/transport/kafka"
	paymentpb "github.com/RoyalTejShinde1993/distributed-payments-platform/pkg/api/payment/v1"
	"google.golang.org/protobuf/proto"
)

type Handler struct {
	paymentpb.UnimplementedPaymentServiceServer
	producer *kafka.Producer
}

func NewHandler(producer *kafka.Producer) *Handler {
	return &Handler{producer: producer}
}

func (h *Handler) ProcessPayment(ctx context.Context, req *paymentpb.PaymentRequest) (*paymentpb.PaymentResponse, error) {
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

	if err := h.producer.Publish(ctx, []byte(req.Id), payload); err != nil {
		return nil, err
	}

	return &paymentpb.PaymentResponse{
		Id:      req.Id,
		Status:  "ACCEPTED",
		Message: "payment submitted",
	}, nil
}

func (h *Handler) StreamPayments(stream paymentpb.PaymentService_StreamPaymentsServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		event := &paymentpb.PaymentEvent{
			Id:        req.Id,
			Amount:    req.Amount,
			Currency:  req.Currency,
			Status:    "PENDING",
			CreatedAt: time.Now().Unix(),
		}

		payload, err := proto.Marshal(event)
		if err != nil {
			return err
		}

		if err := h.producer.Publish(stream.Context(), []byte(req.Id), payload); err != nil {
			return err
		}

		if err := stream.Send(event); err != nil {
			return err
		}
	}
}
