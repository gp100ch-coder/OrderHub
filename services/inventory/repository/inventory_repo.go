package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/orderhub/services/inventory/service"
)

var _ service.InventoryRepository = (*InventoryRepository)(nil)

// InventoryRepository implements inventory persistence with PostgreSQL and Redis
type InventoryRepository struct {
	db    *pgxpool.Pool
	redis *redis.Client
	log   *zap.Logger
}

// NewInventoryRepository creates a new InventoryRepository instance
func NewInventoryRepository(db *pgxpool.Pool, redisClient *redis.Client, log *zap.Logger) *InventoryRepository {
	return &InventoryRepository{
		db:    db,
		redis: redisClient,
		log:   log,
	}
}

// GetByProductID retrieves inventory for a product
func (r *InventoryRepository) GetByProductID(ctx context.Context, productID string) (*service.InventoryItem, error) {
	// Try cache first
	if cached, err := r.getCachedInventory(ctx, productID); err == nil && cached != nil {
		return cached, nil
	}

	query := `
		SELECT product_id, quantity, reserved, version, updated_at
		FROM inventory
		WHERE product_id = $1
	`

	var item service.InventoryItem
	err := r.db.QueryRow(ctx, query, productID).Scan(
		&item.ProductID,
		&item.Quantity,
		&item.Reserved,
		&item.Version,
		&item.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, service.ErrProductNotFound
		}
		return nil, fmt.Errorf("failed to get inventory: %w", err)
	}

	// Cache the result
	r.cacheInventory(ctx, &item)

	return &item, nil
}

// UpdateWithOptimisticLock updates inventory with optimistic locking
func (r *InventoryRepository) UpdateWithOptimisticLock(ctx context.Context, item *service.InventoryItem) error {
	query := `
		UPDATE inventory
		SET quantity = $1, reserved = $2, version = $3, updated_at = $4
		WHERE product_id = $5 AND version = $6
	`

	result, err := r.db.Exec(ctx, query,
		item.Quantity,
		item.Reserved,
		item.Version,
		item.UpdatedAt,
		item.ProductID,
		item.Version-1, // Expected version
	)
	if err != nil {
		return fmt.Errorf("failed to update inventory: %w", err)
	}

	if result.RowsAffected() == 0 {
		return errors.New("optimistic lock failed: version mismatch")
	}

	// Invalidate cache
	r.invalidateInventoryCache(ctx, item.ProductID)

	return nil
}

// CreateReservation creates a new stock reservation
func (r *InventoryRepository) CreateReservation(ctx context.Context, reservation *service.Reservation) error {
	itemsJSON, err := json.Marshal(reservation.Items)
	if err != nil {
		return fmt.Errorf("failed to marshal items: %w", err)
	}

	query := `
		INSERT INTO reservations (id, order_id, items, status, created_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (order_id) DO NOTHING
	`

	_, err = r.db.Exec(ctx, query,
		reservation.ID,
		reservation.OrderID,
		itemsJSON,
		reservation.Status,
		reservation.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("failed to create reservation: %w", err)
	}

	return nil
}

// GetReservationByOrderID retrieves a reservation by order ID
func (r *InventoryRepository) GetReservationByOrderID(ctx context.Context, orderID string) (*service.Reservation, error) {
	query := `
		SELECT id, order_id, items, status, created_at
		FROM reservations
		WHERE order_id = $1
	`

	var reservation service.Reservation
	var itemsJSON []byte

	err := r.db.QueryRow(ctx, query, orderID).Scan(
		&reservation.ID,
		&reservation.OrderID,
		&itemsJSON,
		&reservation.Status,
		&reservation.CreatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errors.New("reservation not found")
		}
		return nil, fmt.Errorf("failed to get reservation: %w", err)
	}

	if err := json.Unmarshal(itemsJSON, &reservation.Items); err != nil {
		return nil, fmt.Errorf("failed to unmarshal items: %w", err)
	}

	return &reservation, nil
}

// UpdateReservationStatus updates the status of a reservation
func (r *InventoryRepository) UpdateReservationStatus(ctx context.Context, orderID, status string) error {
	query := `
		UPDATE reservations
		SET status = $1, updated_at = $2
		WHERE order_id = $3
	`

	result, err := r.db.Exec(ctx, query, status, time.Now(), orderID)
	if err != nil {
		return fmt.Errorf("failed to update reservation status: %w", err)
	}

	if result.RowsAffected() == 0 {
		return errors.New("reservation not found")
	}

	return nil
}

// DeleteReservation deletes a reservation
func (r *InventoryRepository) DeleteReservation(ctx context.Context, orderID string) error {
	query := `DELETE FROM reservations WHERE order_id = $1`

	result, err := r.db.Exec(ctx, query, orderID)
	if err != nil {
		return fmt.Errorf("failed to delete reservation: %w", err)
	}

	if result.RowsAffected() == 0 {
		return errors.New("reservation not found")
	}

	return nil
}

// Helper methods for caching

func (r *InventoryRepository) cacheInventory(ctx context.Context, item *service.InventoryItem) {
	if r.redis == nil {
		return
	}

	itemJSON, err := json.Marshal(item)
	if err != nil {
		r.log.Warn("Failed to marshal inventory for caching", zap.Error(err))
		return
	}

	// Cache for 2 minutes
	err = r.redis.Set(ctx, "inventory:"+item.ProductID, itemJSON, 2*time.Minute).Err()
	if err != nil {
		r.log.Debug("Failed to cache inventory", zap.Error(err))
	}
}

func (r *InventoryRepository) getCachedInventory(ctx context.Context, productID string) (*service.InventoryItem, error) {
	if r.redis == nil {
		return nil, nil
	}

	data, err := r.redis.Get(ctx, "inventory:"+productID).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, err
	}

	var item service.InventoryItem
	if err := json.Unmarshal([]byte(data), &item); err != nil {
		return nil, err
	}

	return &item, nil
}

func (r *InventoryRepository) invalidateInventoryCache(ctx context.Context, productID string) {
	if r.redis == nil {
		return
	}

	r.redis.Del(ctx, "inventory:"+productID)
}

// NewPostgresDB creates a new PostgreSQL connection pool
func NewPostgresDB(host string, port int, dbName, user, password string) (*pgxpool.Pool, error) {
	connString := fmt.Sprintf(
		"host=%s port=%d dbname=%s user=%s password=%s sslmode=disable",
		host, port, dbName, user, password,
	)

	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection string: %w", err)
	}

	config.MaxConns = 25
	config.MinConns = 5
	config.MaxConnLifetime = time.Hour
	config.MaxConnIdleTime = 30 * time.Minute

	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return pool, nil
}

// NewRedisClient creates a new Redis client
func NewRedisClient(host string, port int, password string) *redis.Client {
	client := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", host, port),
		Password: password,
		DB:       0,
	})

	return client
}
