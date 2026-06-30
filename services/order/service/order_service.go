package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/orderhub/shared/circuitbreaker"
)

// Order status constants
const (
	OrderStatusPending    = "pending"
	OrderStatusConfirmed  = "confirmed"
	OrderStatusPaid       = "paid"
	OrderStatusShipped    = "shipped"
	OrderStatusCancelled  = "cancelled"
	OrderStatusFailed     = "failed"
)

// Error definitions
var (
	ErrOrderNotFound      = errors.New("order not found")
	ErrDuplicateOrder     = errors.New("duplicate order")
	ErrInsufficientInventory = errors.New("insufficient inventory")
	ErrPaymentFailed      = errors.New("payment failed")
	ErrInvalidOrderStatus = errors.New("invalid order status")
)

// Order represents an order entity
type Order struct {
	ID             string          `json:"id"`
	UserID         string          `json:"user_id"`
	Status         string          `json:"status"`
	Items          []OrderItem     `json:"items"`
	Shipping       ShippingAddress `json:"shipping"`
	TotalAmount    int64           `json:"total_amount"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
	IdempotencyKey string          `json:"-"`
}

// OrderItem represents an item in an order
type OrderItem struct {
	ProductID string `json:"product_id"`
	Quantity  int    `json:"quantity"`
	Price     int64  `json:"price"`
}

// ShippingAddress represents the shipping address
type ShippingAddress struct {
	Street  string `json:"street"`
	City    string `json:"city"`
	State   string `json:"state"`
	ZipCode string `json:"zip_code"`
	Country string `json:"country"`
}

// CreateOrderCommand represents the command to create an order
type CreateOrderCommand struct {
	UserID         string
	Items          []OrderItem
	Shipping       ShippingAddress
	IdempotencyKey string
}

// ListOrdersQuery represents query parameters for listing orders
type ListOrdersQuery struct {
	UserID string
	Status string
	Page   int
	Limit  int
}

// OrderRepository defines the interface for order persistence
type OrderRepository interface {
	Create(ctx context.Context, order *Order) error
	GetByID(ctx context.Context, id string) (*Order, error)
	GetByIdempotencyKey(ctx context.Context, key string) (*Order, error)
	List(ctx context.Context, query ListOrdersQuery) ([]*Order, int64, error)
	UpdateStatus(ctx context.Context, id, status string) error
}

// InventoryClient defines the interface for inventory service communication
type InventoryClient interface {
	ReserveStock(ctx context.Context, items []OrderItem, orderID string) error
	ReleaseStock(ctx context.Context, items []OrderItem, orderID string) error
	ConfirmStock(ctx context.Context, items []OrderItem, orderID string) error
}

// PaymentClient defines the interface for payment service communication
type PaymentClient interface {
	ProcessPayment(ctx context.Context, orderID string, amount int64, userID string) error
	RefundPayment(ctx context.Context, orderID string, amount int64) error
}

// OrderService handles order business logic with saga orchestration
type OrderService struct {
	repo              OrderRepository
	inventoryClient   InventoryClient
	paymentClient     PaymentClient
	logger            *zap.Logger
	circuitBreaker    *circuitbreaker.CircuitBreaker
}

// NewOrderService creates a new OrderService instance
func NewOrderService(
	repo OrderRepository,
	inventoryClient InventoryClient,
	paymentClient PaymentClient,
	logger *zap.Logger,
	inventoryAddress string,
	paymentAddress string,
) *OrderService {
	cb := circuitbreaker.New(circuitbreaker.Config{
		MaxFailures:   5,
		Timeout:       30 * time.Second,
		HalfOpenLimit: 3,
	})

	return &OrderService{
		repo:            repo,
		inventoryClient: inventoryClient,
		paymentClient:   paymentClient,
		logger:          logger,
		circuitBreaker:  cb,
	}
}

// CreateOrder creates a new order using saga pattern for distributed transaction
func (s *OrderService) CreateOrder(ctx context.Context, cmd CreateOrderCommand) (*Order, error) {
	// Check for idempotency
	if cmd.IdempotencyKey != "" {
		existingOrder, err := s.repo.GetByIdempotencyKey(ctx, cmd.IdempotencyKey)
		if err == nil && existingOrder != nil {
			return existingOrder, ErrDuplicateOrder
		}
	}

	// Calculate total amount
	var totalAmount int64
	for _, item := range cmd.Items {
		totalAmount += item.Price * int64(item.Quantity)
	}

	// Create order entity
	order := &Order{
		ID:             uuid.New().String(),
		UserID:         cmd.UserID,
		Status:         OrderStatusPending,
		Items:          cmd.Items,
		Shipping:       cmd.Shipping,
		TotalAmount:    totalAmount,
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
		IdempotencyKey: cmd.IdempotencyKey,
	}

	// Start saga: Step 1 - Persist order in pending state
	if err := s.repo.Create(ctx, order); err != nil {
		s.logger.Error("Failed to create order in database", zap.Error(err))
		return nil, fmt.Errorf("failed to persist order: %w", err)
	}

	// Step 2 - Reserve inventory (Saga step with compensation)
	if err := s.reserveInventory(ctx, order); err != nil {
		s.logger.Error("Failed to reserve inventory", zap.Error(err))
		// Compensation: No need to rollback DB as order is in pending state
		// A cleanup job can handle stale pending orders
		return nil, ErrInsufficientInventory
	}

	// Step 3 - Process payment (Saga step with compensation)
	if err := s.processPayment(ctx, order); err != nil {
		s.logger.Error("Failed to process payment", zap.Error(err))
		// Compensation: Release reserved inventory
		if releaseErr := s.inventoryClient.ReleaseStock(ctx, order.Items, order.ID); releaseErr != nil {
			s.logger.Error("Failed to release inventory during compensation", zap.Error(releaseErr))
		}
		// Update order status to failed
		if updateErr := s.repo.UpdateStatus(ctx, order.ID, OrderStatusFailed); updateErr != nil {
			s.logger.Error("Failed to update order status to failed", zap.Error(updateErr))
		}
		return nil, ErrPaymentFailed
	}

	// Step 4 - Confirm inventory reservation
	if err := s.inventoryClient.ConfirmStock(ctx, order.Items, order.ID); err != nil {
		s.logger.Error("Failed to confirm inventory", zap.Error(err))
		// Compensation: Refund payment
		if refundErr := s.paymentClient.RefundPayment(ctx, order.ID, order.TotalAmount); refundErr != nil {
			s.logger.Error("Failed to refund payment during compensation", zap.Error(refundErr))
		}
		if releaseErr := s.inventoryClient.ReleaseStock(ctx, order.Items, order.ID); releaseErr != nil {
			s.logger.Error("Failed to release inventory during compensation", zap.Error(releaseErr))
		}
		if updateErr := s.repo.UpdateStatus(ctx, order.ID, OrderStatusFailed); updateErr != nil {
			s.logger.Error("Failed to update order status to failed", zap.Error(updateErr))
		}
		return nil, fmt.Errorf("failed to confirm inventory: %w", err)
	}

	// Step 5 - Update order status to confirmed
	if err := s.repo.UpdateStatus(ctx, order.ID, OrderStatusConfirmed); err != nil {
		s.logger.Error("Failed to update order status", zap.Error(err))
		return nil, fmt.Errorf("failed to finalize order: %w", err)
	}

	order.Status = OrderStatusConfirmed
	order.UpdatedAt = time.Now()

	s.logger.Info("Order created successfully",
		zap.String("order_id", order.ID),
		zap.String("user_id", order.UserID),
		zap.Int64("total_amount", order.TotalAmount))

	return order, nil
}

// GetOrder retrieves an order by ID
func (s *OrderService) GetOrder(ctx context.Context, orderID string) (*Order, error) {
	order, err := s.repo.GetByID(ctx, orderID)
	if err != nil {
		return nil, err
	}
	if order == nil {
		return nil, ErrOrderNotFound
	}
	return order, nil
}

// ListOrders lists orders with pagination and filtering
func (s *OrderService) ListOrders(ctx context.Context, query ListOrdersQuery) ([]*Order, int64, error) {
	if query.Page < 1 {
		query.Page = 1
	}
	if query.Limit < 1 {
		query.Limit = 20
	}
	if query.Limit > 100 {
		query.Limit = 100
	}

	return s.repo.List(ctx, query)
}

// CancelOrder cancels an order (compensating transaction)
func (s *OrderService) CancelOrder(ctx context.Context, orderID string) error {
	order, err := s.repo.GetByID(ctx, orderID)
	if err != nil {
		return err
	}
	if order == nil {
		return ErrOrderNotFound
	}

	if order.Status == OrderStatusCancelled || order.Status == OrderStatusFailed {
		return ErrInvalidOrderStatus
	}

	// Release inventory
	if err := s.inventoryClient.ReleaseStock(ctx, order.Items, order.ID); err != nil {
		s.logger.Error("Failed to release inventory during cancellation", zap.Error(err))
	}

	// Refund payment if already paid
	if order.Status == OrderStatusPaid || order.Status == OrderStatusConfirmed {
		if err := s.paymentClient.RefundPayment(ctx, order.ID, order.TotalAmount); err != nil {
			s.logger.Error("Failed to refund payment during cancellation", zap.Error(err))
		}
	}

	// Update order status
	if err := s.repo.UpdateStatus(ctx, orderID, OrderStatusCancelled); err != nil {
		return err
	}

	s.logger.Info("Order cancelled", zap.String("order_id", orderID))
	return nil
}

func (s *OrderService) reserveInventory(ctx context.Context, order *Order) error {
	return s.circuitBreaker.Execute(func() error {
		return s.inventoryClient.ReserveStock(ctx, order.Items, order.ID)
	})
}

func (s *OrderService) processPayment(ctx context.Context, order *Order) error {
	return s.circuitBreaker.Execute(func() error {
		return s.paymentClient.ProcessPayment(ctx, order.ID, order.TotalAmount, order.UserID)
	})
}
