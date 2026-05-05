package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/RoyalTejShinde1993/distributed-payments-platform/internal/payments"
	paymentpb "github.com/RoyalTejShinde1993/distributed-payments-platform/pkg/api/payment/v1"
)

type httpPaymentRequest struct {
	Id         string  `json:"id"`
	Amount     float64 `json:"amount"`
	Currency   string  `json:"currency"`
	CustomerID string  `json:"customer_id"`
}

type httpPaymentResponse struct {
	Id      string `json:"id"`
	Status  string `json:"status"`
	Message string `json:"message"`
}

func main() {
	log.Println("Starting Distributed Payments Platform")

	cfg := payments.Config{
		GRPCAddress:   ":50051",
		KafkaBrokers:  []string{"localhost:9092"},
		PaymentTopic:  "payments",
		ConsumerGroup: "payment-service",
	}

	svc := payments.NewService(cfg)
	if err := svc.Start(context.Background()); err != nil {
		log.Fatalf("service start failed: %v", err)
	}
	defer svc.Stop()

	httpSrv := &http.Server{
		Addr:    ":8080",
		Handler: newHTTPHandler(svc),
	}

	go func() {
		log.Println("HTTP gateway listening on :8080")
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http server failed: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	<-stop
	log.Println("Shutting down platform")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(ctx)
}

func newHTTPHandler(svc *payments.Service) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"service": "distributed-payments-platform",
			"status":  "ok",
			"routes": []string{
				"/healthz",
				"/payment",
			},
		})
	})

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	mux.HandleFunc("/payment", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req httpPaymentRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request body", http.StatusBadRequest)
			return
		}

		resp, err := svc.ProcessPayment(r.Context(), &paymentpb.PaymentRequest{
			Id:         req.Id,
			Amount:     req.Amount,
			Currency:   req.Currency,
			CustomerId: req.CustomerID,
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(httpPaymentResponse{
			Id:      resp.Id,
			Status:  resp.Status,
			Message: resp.Message,
		})
	})

	return mux
}
