package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/orderhub/proto/pb"
)

// Error definitions
var (
	ErrPaymentNotFound    = errors.New("payment not found")
	ErrDuplicatePayment   = errors.New("duplicate payment request")
	ErrPaymentFailed      = errors.New("payment processing failed")
	ErrRefundFailed       = errors.New("refund processing failed")
	ErrInvalidAmount      = errors.New("invalid amount")
	ErrIdempotencyKeyUsed = errors.New("idempotency key already used")
)

// Payment status constants
const (
	PaymentStatusPending   = "pending"
	PaymentStatusCompleted = "completed"
	PaymentStatusFailed    = "failed"
	PaymentStatusRefunded  = "refunded"
)

// Payment represents a payment transaction
type Payment struct {
	ID              string
	OrderID         string
	UserID          string
	Amount          int64
	Status          string
	IdempotencyKey  string
	CreatedAt       time.Time
	UpdatedAt       time.Time
	RefundAmount    int64
}

// PaymentRepository defines the interface for payment persistence
type PaymentRepository interface {
	Create(ctx context.Context, payment *Payment) error
	GetByOrderID(ctx context.Context, orderID string) (*Payment, error)
	GetByIdempotencyKey(ctx context.Context, key string) (*Payment, error)
	UpdateStatus(ctx context.Context, id, status string) error
	CreateRefund(ctx context.Context, paymentID string, amount int64) error
}

// InMemoryPaymentRepository is an in-memory implementation for demo purposes
type InMemoryPaymentRepository struct {
	payments         sync.Map
	idempotencyKeys  sync.Map
}

func NewInMemoryPaymentRepository() *InMemoryPaymentRepository {
	return &InMemoryPaymentRepository{}
}

func (r *InMemoryPaymentRepository) Create(ctx context.Context, payment *Payment) error {
	if payment.IdempotencyKey != "" {
		if _, exists := r.idempotencyKeys.Load(payment.IdempotencyKey); exists {
			return ErrDuplicatePayment
		}
		r.idempotencyKeys.Store(payment.IdempotencyKey, payment.ID)
	}
	r.payments.Store(payment.ID, payment)
	r.payments.Store("order:"+payment.OrderID, payment.ID)
	return nil
}

func (r *InMemoryPaymentRepository) GetByOrderID(ctx context.Context, orderID string) (*Payment, error) {
	if id, ok := r.payments.Load("order:" + orderID); ok {
		if payment, ok := r.payments.Load(id.(string)); ok {
			return payment.(*Payment), nil
		}
	}
	return nil, ErrPaymentNotFound
}

func (r *InMemoryPaymentRepository) GetByIdempotencyKey(ctx context.Context, key string) (*Payment, error) {
	if id, ok := r.idempotencyKeys.Load(key); ok {
		if payment, ok := r.payments.Load(id.(string)); ok {
			return payment.(*Payment), nil
		}
	}
	return nil, ErrPaymentNotFound
}

func (r *InMemoryPaymentRepository) UpdateStatus(ctx context.Context, id, status string) error {
	if payment, ok := r.payments.Load(id); ok {
		p := payment.(*Payment)
		p.Status = status
		p.UpdatedAt = time.Now()
		r.payments.Store(id, p)
		return nil
	}
	return ErrPaymentNotFound
}

func (r *InMemoryPaymentRepository) CreateRefund(ctx context.Context, paymentID string, amount int64) error {
	if payment, ok := r.payments.Load(paymentID); ok {
		p := payment.(*Payment)
		p.RefundAmount += amount
		if p.RefundAmount >= p.Amount {
			p.Status = PaymentStatusRefunded
		}
		p.UpdatedAt = time.Now()
		r.payments.Store(paymentID, p)
		return nil
	}
	return ErrPaymentNotFound
}

// PaymentService handles payment business logic
type PaymentService struct {
	repo   PaymentRepository
	logger *zap.Logger
	pb.UnimplementedPaymentServiceServer
}

// NewPaymentService creates a new PaymentService instance
func NewPaymentService(logger *zap.Logger) *PaymentService {
	return &PaymentService{
		repo:   NewInMemoryPaymentRepository(),
		logger: logger,
	}
}

// ProcessPayment processes a payment with idempotency support (gRPC implementation)
func (s *PaymentService) ProcessPayment(ctx context.Context, req *pb.ProcessPaymentRequest) (*pb.ProcessPaymentResponse, error) {
	s.logger.Info("Processing payment",
		zap.String("order_id", req.OrderId),
		zap.String("user_id", req.UserId),
		zap.Int64("amount", req.Amount),
		zap.String("idempotency_key", req.IdempotencyKey))

	if req.Amount <= 0 {
		return &pb.ProcessPaymentResponse{
			Success: false,
			Message: "Invalid amount",
		}, ErrInvalidAmount
	}

	// Check idempotency
	if req.IdempotencyKey != "" {
		existingPayment, err := s.repo.GetByIdempotencyKey(ctx, req.IdempotencyKey)
		if err == nil && existingPayment != nil {
			s.logger.Info("Duplicate payment request detected",
				zap.String("existing_payment_id", existingPayment.ID))
			
			if existingPayment.Status == PaymentStatusCompleted {
				return &pb.ProcessPaymentResponse{
					Success:   true,
					Message:   "Payment already processed",
					PaymentId: existingPayment.ID,
				}, nil
			}
		}
	}

	// Check if order already has a payment
	existingPayment, _ := s.repo.GetByOrderID(ctx, req.OrderId)
	if existingPayment != nil && existingPayment.Status == PaymentStatusCompleted {
		return &pb.ProcessPaymentResponse{
			Success:   true,
			Message:   "Payment already exists for this order",
			PaymentId: existingPayment.ID,
		}, nil
	}

	// Simulate payment processing (in production, integrate with Stripe/PayPal)
	paymentID := generatePaymentID()
	payment := &Payment{
		ID:             paymentID,
		OrderID:        req.OrderId,
		UserID:         req.UserId,
		Amount:         req.Amount,
		Status:         PaymentStatusPending,
		IdempotencyKey: req.IdempotencyKey,
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
	}

	// Store pending payment
	if err := s.repo.Create(ctx, payment); err != nil {
		if errors.Is(err, ErrDuplicatePayment) {
			return &pb.ProcessPaymentResponse{
				Success: false,
				Message: "Duplicate payment request",
			}, nil
		}
		s.logger.Error("Failed to create payment record", zap.Error(err))
		return &pb.ProcessPaymentResponse{
			Success: false,
			Message: "Failed to create payment record",
		}, err
	}

	// Simulate payment gateway call
	if err := s.simulatePaymentGateway(payment); err != nil {
		s.logger.Error("Payment gateway failed", zap.Error(err))
		if updateErr := s.repo.UpdateStatus(ctx, paymentID, PaymentStatusFailed); updateErr != nil {
			s.logger.Error("Failed to update payment status", zap.Error(updateErr))
		}
		return &pb.ProcessPaymentResponse{
			Success: false,
			Message: fmt.Sprintf("Payment failed: %v", err),
		}, ErrPaymentFailed
	}

	// Update payment status to completed
	if err := s.repo.UpdateStatus(ctx, paymentID, PaymentStatusCompleted); err != nil {
		s.logger.Error("Failed to update payment status", zap.Error(err))
		return &pb.ProcessPaymentResponse{
			Success: false,
			Message: "Failed to finalize payment",
		}, err
	}

	s.logger.Info("Payment processed successfully",
		zap.String("payment_id", paymentID),
		zap.String("order_id", req.OrderId))

	return &pb.ProcessPaymentResponse{
		Success:   true,
		Message:   "Payment processed successfully",
		PaymentId: paymentID,
	}, nil
}

// RefundPayment processes a refund (gRPC implementation)
func (s *PaymentService) RefundPayment(ctx context.Context, req *pb.RefundPaymentRequest) (*pb.RefundPaymentResponse, error) {
	s.logger.Info("Processing refund",
		zap.String("order_id", req.OrderId),
		zap.Int64("amount", req.Amount))

	payment, err := s.repo.GetByOrderID(ctx, req.OrderId)
	if err != nil {
		return &pb.RefundPaymentResponse{
			Success: false,
			Message: "Payment not found",
		}, ErrPaymentNotFound
	}

	if payment.Status != PaymentStatusCompleted {
		return &pb.RefundPaymentResponse{
			Success: false,
			Message: "Payment not in completed state",
		}, nil
	}

	refundAmount := req.Amount
	if refundAmount <= 0 {
		refundAmount = payment.Amount // Full refund
	}

	if refundAmount > payment.Amount-payment.RefundAmount {
		return &pb.RefundPaymentResponse{
			Success: false,
			Message: "Refund amount exceeds payment amount",
		}, ErrInvalidAmount
	}

	// Simulate refund processing
	if err := s.simulateRefundGateway(payment, refundAmount); err != nil {
		s.logger.Error("Refund gateway failed", zap.Error(err))
		return &pb.RefundPaymentResponse{
			Success: false,
			Message: fmt.Sprintf("Refund failed: %v", err),
		}, ErrRefundFailed
	}

	// Record refund
	if err := s.repo.CreateRefund(ctx, payment.ID, refundAmount); err != nil {
		s.logger.Error("Failed to record refund", zap.Error(err))
		return &pb.RefundPaymentResponse{
			Success: false,
			Message: "Failed to record refund",
		}, err
	}

	s.logger.Info("Refund processed successfully",
		zap.String("payment_id", payment.ID),
		zap.Int64("refund_amount", refundAmount))

	return &pb.RefundPaymentResponse{
		Success: true,
		Message: "Refund processed successfully",
	}, nil
}

// GetPaymentStatus returns the status of a payment
func (s *PaymentService) GetPaymentStatus(ctx context.Context, req *pb.GetPaymentStatusRequest) (*pb.GetPaymentStatusResponse, error) {
	payment, err := s.repo.GetByOrderID(ctx, req.OrderId)
	if err != nil {
		return &pb.GetPaymentStatusResponse{
			Found: false,
		}, ErrPaymentNotFound
	}

	return &pb.GetPaymentStatusResponse{
		Found:       true,
		PaymentId:   payment.ID,
		Status:      payment.Status,
		Amount:      payment.Amount,
		RefundAmount: payment.RefundAmount,
	}, nil
}

// Helper methods

func (s *PaymentService) simulatePaymentGateway(payment *Payment) error {
	// Simulate network delay
	time.Sleep(100 * time.Millisecond)

	// Simulate occasional failures (5% failure rate)
	if rand.Float32() < 0.05 {
		return errors.New("simulated payment gateway failure")
	}

	return nil
}

func (s *PaymentService) simulateRefundGateway(payment *Payment, amount int64) error {
	// Simulate network delay
	time.Sleep(100 * time.Millisecond)

	// Simulate occasional failures (3% failure rate)
	if rand.Float32() < 0.03 {
		return errors.New("simulated refund gateway failure")
	}

	return nil
}

func generatePaymentID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return "pay_" + hex.EncodeToString(b)
}

// Client-side methods for use by other services

// ProcessPaymentClient is used by order service to call payment service
func (s *PaymentService) ProcessPaymentClient(ctx context.Context, orderID, userID string, amount int64, idempotencyKey string) error {
	req := &pb.ProcessPaymentRequest{
		OrderId:        orderID,
		UserId:         userID,
		Amount:         amount,
		IdempotencyKey: idempotencyKey,
	}

	resp, err := s.ProcessPayment(ctx, req)
	if err != nil {
		return err
	}

	if !resp.Success {
		return fmt.Errorf("payment failed: %s", resp.Message)
	}

	return nil
}

// RefundPaymentClient refunds a payment
func (s *PaymentService) RefundPaymentClient(ctx context.Context, orderID string, amount int64) error {
	req := &pb.RefundPaymentRequest{
		OrderId: orderID,
		Amount:  amount,
	}

	resp, err := s.RefundPayment(ctx, req)
	if err != nil {
		return err
	}

	if !resp.Success {
		return fmt.Errorf("refund failed: %s", resp.Message)
	}

	return nil
}
