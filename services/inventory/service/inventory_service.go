package service

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/orderhub/proto/pb"
)

// Error definitions
var (
	ErrInsufficientStock = errors.New("insufficient stock")
	ErrProductNotFound   = errors.New("product not found")
	ErrInvalidQuantity   = errors.New("invalid quantity")
)

// InventoryItem represents a product's inventory
type InventoryItem struct {
	ProductID   string
	Quantity    int
	Reserved    int
	Version     int64 // For optimistic locking
	UpdatedAt   time.Time
}

// Reservation represents a stock reservation
type Reservation struct {
	ID        string
	OrderID   string
	Items     []ReservedItem
	CreatedAt time.Time
	Status    string // reserved, confirmed, released
}

// ReservedItem represents a reserved item
type ReservedItem struct {
	ProductID string
	Quantity  int
}

// InventoryRepository defines the interface for inventory persistence
type InventoryRepository interface {
	GetByProductID(ctx context.Context, productID string) (*InventoryItem, error)
	UpdateWithOptimisticLock(ctx context.Context, item *InventoryItem) error
	CreateReservation(ctx context.Context, reservation *Reservation) error
	GetReservationByOrderID(ctx context.Context, orderID string) (*Reservation, error)
	UpdateReservationStatus(ctx context.Context, orderID, status string) error
	DeleteReservation(ctx context.Context, orderID string) error
}

// InventoryService handles inventory business logic
type InventoryService struct {
	repo           InventoryRepository
	logger         *zap.Logger
	reservations   sync.Map // In-memory cache for reservations
	pb.UnimplementedInventoryServiceServer
}

// NewInventoryService creates a new InventoryService instance
func NewInventoryService(repo InventoryRepository, logger *zap.Logger) *InventoryService {
	return &InventoryService{
		repo:   repo,
		logger: logger,
	}
}

// ReserveStock reserves stock for an order (gRPC implementation)
func (s *InventoryService) ReserveStock(ctx context.Context, req *pb.ReserveStockRequest) (*pb.ReserveStockResponse, error) {
	s.logger.Info("Reserving stock",
		zap.String("order_id", req.OrderId),
		zap.Int("items_count", len(req.Items)))

	reservation := &Reservation{
		ID:        fmt.Sprintf("res_%d", time.Now().UnixNano()),
		OrderID:   req.OrderId,
		CreatedAt: time.Now(),
		Status:    "reserved",
		Items:     make([]ReservedItem, len(req.Items)),
	}

	// Process each item
	for i, item := range req.Items {
		if item.Quantity <= 0 {
			return nil, ErrInvalidQuantity
		}

		inventory, err := s.repo.GetByProductID(ctx, item.ProductId)
		if err != nil {
			if errors.Is(err, ErrProductNotFound) {
				// Create new inventory entry with zero stock
				inventory = &InventoryItem{
					ProductID: item.ProductId,
					Quantity:  0,
					Version:   1,
				}
			} else {
				return nil, fmt.Errorf("failed to get inventory: %w", err)
			}
		}

		// Check available stock
		available := inventory.Quantity - inventory.Reserved
		if available < item.Quantity {
			s.logger.Warn("Insufficient stock",
				zap.String("product_id", item.ProductId),
				zap.Int("requested", item.Quantity),
				zap.Int("available", available))
			return &pb.ReserveStockResponse{
				Success: false,
				Message: fmt.Sprintf("Insufficient stock for product %s", item.ProductId),
			}, nil
		}

		// Update reservation with optimistic locking
		newInventory := &InventoryItem{
			ProductID: inventory.ProductID,
			Quantity:  inventory.Quantity,
			Reserved:  inventory.Reserved + item.Quantity,
			Version:   inventory.Version + 1,
			UpdatedAt: time.Now(),
		}

		if err := s.repo.UpdateWithOptimisticLock(ctx, newInventory); err != nil {
			s.logger.Warn("Optimistic lock failure, retrying",
				zap.String("product_id", item.ProductId),
				zap.Error(err))
			return nil, fmt.Errorf("concurrent modification detected")
		}

		reservation.Items[i] = ReservedItem{
			ProductID: item.ProductId,
			Quantity:  item.Quantity,
		}
	}

	// Store reservation
	if err := s.repo.CreateReservation(ctx, reservation); err != nil {
		s.logger.Error("Failed to create reservation", zap.Error(err))
		// Rollback reservations would be needed here in production
		return nil, fmt.Errorf("failed to create reservation: %w", err)
	}

	s.reservations.Store(req.OrderId, reservation)

	s.logger.Info("Stock reserved successfully", zap.String("order_id", req.OrderId))

	return &pb.ReserveStockResponse{
		Success: true,
		Message: "Stock reserved successfully",
	}, nil
}

// ReleaseStock releases reserved stock (gRPC implementation)
func (s *InventoryService) ReleaseStock(ctx context.Context, req *pb.ReleaseStockRequest) (*pb.ReleaseStockResponse, error) {
	s.logger.Info("Releasing stock", zap.String("order_id", req.OrderId))

	reservation, err := s.repo.GetReservationByOrderID(ctx, req.OrderId)
	if err != nil {
		return nil, fmt.Errorf("reservation not found: %w", err)
	}

	if reservation.Status != "reserved" {
		return &pb.ReleaseStockResponse{
			Success: false,
			Message: "Reservation already processed",
		}, nil
	}

	// Release each item
	for _, item := range reservation.Items {
		inventory, err := s.repo.GetByProductID(ctx, item.ProductID)
		if err != nil {
			s.logger.Warn("Failed to get inventory during release",
				zap.String("product_id", item.ProductID),
				zap.Error(err))
			continue
		}

		newInventory := &InventoryItem{
			ProductID: inventory.ProductID,
			Quantity:  inventory.Quantity,
			Reserved:  inventory.Reserved - item.Quantity,
			Version:   inventory.Version + 1,
			UpdatedAt: time.Now(),
		}

		if err := s.repo.UpdateWithOptimisticLock(ctx, newInventory); err != nil {
			s.logger.Warn("Failed to update inventory during release",
				zap.String("product_id", item.ProductID),
				zap.Error(err))
		}
	}

	// Update reservation status
	if err := s.repo.UpdateReservationStatus(ctx, req.OrderId, "released"); err != nil {
		s.logger.Error("Failed to update reservation status", zap.Error(err))
	}

	s.reservations.Delete(req.OrderId)

	s.logger.Info("Stock released successfully", zap.String("order_id", req.OrderId))

	return &pb.ReleaseStockResponse{
		Success: true,
		Message: "Stock released successfully",
	}, nil
}

// ConfirmStock confirms a reservation and permanently deducts stock
func (s *InventoryService) ConfirmStock(ctx context.Context, req *pb.ConfirmStockRequest) (*pb.ConfirmStockResponse, error) {
	s.logger.Info("Confirming stock", zap.String("order_id", req.OrderId))

	reservation, err := s.repo.GetReservationByOrderID(ctx, req.OrderId)
	if err != nil {
		return nil, fmt.Errorf("reservation not found: %w", err)
	}

	if reservation.Status != "reserved" {
		return &pb.ConfirmStockResponse{
			Success: false,
			Message: "Reservation not in reserved state",
		}, nil
	}

	// Permanently deduct stock for each item
	for _, item := range reservation.Items {
		inventory, err := s.repo.GetByProductID(ctx, item.ProductID)
		if err != nil {
			s.logger.Warn("Failed to get inventory during confirm",
				zap.String("product_id", item.ProductID),
				zap.Error(err))
			continue
		}

		newInventory := &InventoryItem{
			ProductID: inventory.ProductID,
			Quantity:  inventory.Quantity - item.Quantity,
			Reserved:  inventory.Reserved - item.Quantity,
			Version:   inventory.Version + 1,
			UpdatedAt: time.Now(),
		}

		if err := s.repo.UpdateWithOptimisticLock(ctx, newInventory); err != nil {
			s.logger.Warn("Failed to update inventory during confirm",
				zap.String("product_id", item.ProductID),
				zap.Error(err))
		}
	}

	// Update reservation status
	if err := s.repo.UpdateReservationStatus(ctx, req.OrderId, "confirmed"); err != nil {
		s.logger.Error("Failed to update reservation status", zap.Error(err))
	}

	s.reservations.Delete(req.OrderId)

	s.logger.Info("Stock confirmed successfully", zap.String("order_id", req.OrderId))

	return &pb.ConfirmStockResponse{
		Success: true,
		Message: "Stock confirmed successfully",
	}, nil
}

// GetStock returns current stock level for a product
func (s *InventoryService) GetStock(ctx context.Context, req *pb.GetStockRequest) (*pb.GetStockResponse, error) {
	inventory, err := s.repo.GetByProductID(ctx, req.ProductId)
	if err != nil {
		if errors.Is(err, ErrProductNotFound) {
			return &pb.GetStockResponse{
				ProductId: req.ProductId,
				Available: 0,
				Reserved:  0,
				Total:     0,
			}, nil
		}
		return nil, err
	}

	return &pb.GetStockResponse{
		ProductId: req.ProductId,
		Available: int32(inventory.Quantity - inventory.Reserved),
		Reserved:  int32(inventory.Reserved),
		Total:     int32(inventory.Quantity),
	}, nil
}

// HandleOrderEvent processes order events from Kafka
func (s *InventoryService) HandleOrderEvent(ctx context.Context, msg interface{}) error {
	// Implementation depends on Kafka message format
	// This is a placeholder for event handling logic
	s.logger.Debug("Received order event", zap.Any("message", msg))
	return nil
}

// Client-side methods for use by other services

// ReserveStockClient is used by order service to call inventory service
func (s *InventoryService) ReserveStockClient(ctx context.Context, items []struct {
	ProductID string
	Quantity  int
}, orderID string) error {
	pbItems := make([]*pb.StockItem, len(items))
	for i, item := range items {
		pbItems[i] = &pb.StockItem{
			ProductId: item.ProductID,
			Quantity:  int32(item.Quantity),
		}
	}

	req := &pb.ReserveStockRequest{
		OrderId: orderID,
		Items:   pbItems,
	}

	resp, err := s.ReserveStock(ctx, req)
	if err != nil {
		return err
	}

	if !resp.Success {
		return fmt.Errorf("reservation failed: %s", resp.Message)
	}

	return nil
}

// ReleaseStockClient releases stock for an order
func (s *InventoryService) ReleaseStockClient(ctx context.Context, items []struct {
	ProductID string
	Quantity  int
}, orderID string) error {
	req := &pb.ReleaseStockRequest{
		OrderId: orderID,
	}

	resp, err := s.ReleaseStock(ctx, req)
	if err != nil {
		return err
	}

	if !resp.Success {
		return fmt.Errorf("release failed: %s", resp.Message)
	}

	return nil
}

// ConfirmStockClient confirms a stock reservation
func (s *InventoryService) ConfirmStockClient(ctx context.Context, items []struct {
	ProductID string
	Quantity  int
}, orderID string) error {
	req := &pb.ConfirmStockRequest{
		OrderId: orderID,
	}

	resp, err := s.ConfirmStock(ctx, req)
	if err != nil {
		return err
	}

	if !resp.Success {
		return fmt.Errorf("confirmation failed: %s", resp.Message)
	}

	return nil
}
