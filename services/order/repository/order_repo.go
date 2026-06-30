package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/orderhub/services/order/service"
)

var _ service.OrderRepository = (*OrderRepository)(nil)

// OrderRepository implements order persistence with PostgreSQL
type OrderRepository struct {
	db    *pgxpool.Pool
	redis *redis.Client
	log   *zap.Logger
}

// NewOrderRepository creates a new OrderRepository instance
func NewOrderRepository(db *pgxpool.Pool, log *zap.Logger) *OrderRepository {
	return &OrderRepository{
		db:  db,
		log: log,
	}
}

// Create persists a new order in the database
func (r *OrderRepository) Create(ctx context.Context, order *service.Order) error {
	itemsJSON, err := json.Marshal(order.Items)
	if err != nil {
		return fmt.Errorf("failed to marshal items: %w", err)
	}

	shippingJSON, err := json.Marshal(order.Shipping)
	if err != nil {
		return fmt.Errorf("failed to marshal shipping: %w", err)
	}

	query := `
		INSERT INTO orders (
			id, user_id, status, items, shipping, total_amount, 
			created_at, updated_at, idempotency_key
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`

	_, err = r.db.Exec(ctx, query,
		order.ID,
		order.UserID,
		order.Status,
		itemsJSON,
		shippingJSON,
		order.TotalAmount,
		order.CreatedAt,
		order.UpdatedAt,
		order.IdempotencyKey,
	)
	if err != nil {
		// Check for unique constraint violation on idempotency_key
		if strings.Contains(err.Error(), "unique") || strings.Contains(err.Error(), "duplicate") {
			return errors.New("duplicate order")
		}
		return fmt.Errorf("failed to insert order: %w", err)
	}

	// Cache idempotency key if present
	if order.IdempotencyKey != "" {
		r.cacheIdempotencyKey(ctx, order.IdempotencyKey, order.ID)
	}

	r.log.Debug("Order created in database", zap.String("order_id", order.ID))
	return nil
}

// GetByID retrieves an order by its ID
func (r *OrderRepository) GetByID(ctx context.Context, id string) (*service.Order, error) {
	// Try cache first
	if cached, err := r.getCachedOrder(ctx, id); err == nil && cached != nil {
		return cached, nil
	}

	query := `
		SELECT id, user_id, status, items, shipping, total_amount, 
		       created_at, updated_at, idempotency_key
		FROM orders
		WHERE id = $1
	`

	var order service.Order
	var itemsJSON, shippingJSON []byte

	err := r.db.QueryRow(ctx, query, id).Scan(
		&order.ID,
		&order.UserID,
		&order.Status,
		&itemsJSON,
		&shippingJSON,
		&order.TotalAmount,
		&order.CreatedAt,
		&order.UpdatedAt,
		&order.IdempotencyKey,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get order: %w", err)
	}

	if err := json.Unmarshal(itemsJSON, &order.Items); err != nil {
		return nil, fmt.Errorf("failed to unmarshal items: %w", err)
	}

	if err := json.Unmarshal(shippingJSON, &order.Shipping); err != nil {
		return nil, fmt.Errorf("failed to unmarshal shipping: %w", err)
	}

	// Cache the order
	r.cacheOrder(ctx, &order)

	return &order, nil
}

// GetByIdempotencyKey retrieves an order by its idempotency key
func (r *OrderRepository) GetByIdempotencyKey(ctx context.Context, key string) (*service.Order, error) {
	if key == "" {
		return nil, nil
	}

	// Try Redis cache first
	cachedOrderID, err := r.redis.Get(ctx, "idempotency:"+key).Result()
	if err == nil && cachedOrderID != "" {
		return r.GetByID(ctx, cachedOrderID)
	}

	query := `
		SELECT id, user_id, status, items, shipping, total_amount, 
		       created_at, updated_at, idempotency_key
		FROM orders
		WHERE idempotency_key = $1
	`

	var order service.Order
	var itemsJSON, shippingJSON []byte

	err = r.db.QueryRow(ctx, query, key).Scan(
		&order.ID,
		&order.UserID,
		&order.Status,
		&itemsJSON,
		&shippingJSON,
		&order.TotalAmount,
		&order.CreatedAt,
		&order.UpdatedAt,
		&order.IdempotencyKey,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get order by idempotency key: %w", err)
	}

	if err := json.Unmarshal(itemsJSON, &order.Items); err != nil {
		return nil, fmt.Errorf("failed to unmarshal items: %w", err)
	}

	if err := json.Unmarshal(shippingJSON, &order.Shipping); err != nil {
		return nil, fmt.Errorf("failed to unmarshal shipping: %w", err)
	}

	// Cache the idempotency key mapping
	r.cacheIdempotencyKey(ctx, key, order.ID)

	return &order, nil
}

// List retrieves orders with pagination and filtering
func (r *OrderRepository) List(ctx context.Context, query service.ListOrdersQuery) ([]*service.Order, int64, error) {
	// Build dynamic query based on filters
	whereClauses := []string{}
	args := []interface{}{}
	argIndex := 1

	if query.UserID != "" {
		whereClauses = append(whereClauses, fmt.Sprintf("user_id = $%d", argIndex))
		args = append(args, query.UserID)
		argIndex++
	}

	if query.Status != "" {
		whereClauses = append(whereClauses, fmt.Sprintf("status = $%d", argIndex))
		args = append(args, query.Status)
		argIndex++
	}

	whereClause := ""
	if len(whereClauses) > 0 {
		whereClause = "WHERE " + strings.Join(whereClauses, " AND ")
	}

	// Get total count
	countQuery := fmt.Sprintf(`SELECT COUNT(*) FROM orders %s`, whereClause)
	var total int64
	err := r.db.QueryRow(ctx, countQuery, args...).Scan(&total)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to count orders: %w", err)
	}

	if total == 0 {
		return []*service.Order{}, 0, nil
	}

	// Get paginated results
	offset := (query.Page - 1) * query.Limit
	selectQuery := fmt.Sprintf(`
		SELECT id, user_id, status, items, shipping, total_amount, 
		       created_at, updated_at, idempotency_key
		FROM orders %s
		ORDER BY created_at DESC
		LIMIT $%d OFFSET $%d
	`, whereClause, argIndex, argIndex+1)

	args = append(args, query.Limit, offset)

	rows, err := r.db.Query(ctx, selectQuery, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to list orders: %w", err)
	}
	defer rows.Close()

	orders := make([]*service.Order, 0, query.Limit)
	for rows.Next() {
		var order service.Order
		var itemsJSON, shippingJSON []byte

		err := rows.Scan(
			&order.ID,
			&order.UserID,
			&order.Status,
			&itemsJSON,
			&shippingJSON,
			&order.TotalAmount,
			&order.CreatedAt,
			&order.UpdatedAt,
			&order.IdempotencyKey,
		)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to scan order: %w", err)
		}

		if err := json.Unmarshal(itemsJSON, &order.Items); err != nil {
			return nil, 0, fmt.Errorf("failed to unmarshal items: %w", err)
		}

		if err := json.Unmarshal(shippingJSON, &order.Shipping); err != nil {
			return nil, 0, fmt.Errorf("failed to unmarshal shipping: %w", err)
		}

		orders = append(orders, &order)
	}

	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("error iterating orders: %w", err)
	}

	return orders, total, nil
}

// UpdateStatus updates the status of an order
func (r *OrderRepository) UpdateStatus(ctx context.Context, id, status string) error {
	query := `UPDATE orders SET status = $1, updated_at = $2 WHERE id = $3`

	result, err := r.db.Exec(ctx, query, status, time.Now(), id)
	if err != nil {
		return fmt.Errorf("failed to update order status: %w", err)
	}

	if result.RowsAffected() == 0 {
		return errors.New("order not found")
	}

	// Invalidate cache
	r.invalidateOrderCache(ctx, id)

	r.log.Debug("Order status updated",
		zap.String("order_id", id),
		zap.String("new_status", status))

	return nil
}

// Helper methods for caching

func (r *OrderRepository) cacheOrder(ctx context.Context, order *service.Order) {
	if r.redis == nil {
		return
	}

	orderJSON, err := json.Marshal(order)
	if err != nil {
		r.log.Warn("Failed to marshal order for caching", zap.Error(err))
		return
	}

	// Cache for 5 minutes
	err = r.redis.Set(ctx, "order:"+order.ID, orderJSON, 5*time.Minute).Err()
	if err != nil {
		r.log.Debug("Failed to cache order", zap.Error(err))
	}
}

func (r *OrderRepository) getCachedOrder(ctx context.Context, id string) (*service.Order, error) {
	if r.redis == nil {
		return nil, nil
	}

	data, err := r.redis.Get(ctx, "order:"+id).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, err
	}

	var order service.Order
	if err := json.Unmarshal([]byte(data), &order); err != nil {
		return nil, err
	}

	return &order, nil
}

func (r *OrderRepository) invalidateOrderCache(ctx context.Context, orderID string) {
	if r.redis == nil {
		return
	}

	// Delete order cache
	r.redis.Del(ctx, "order:"+orderID)
}

func (r *OrderRepository) cacheIdempotencyKey(ctx context.Context, key, orderID string) {
	if r.redis == nil {
		return
	}

	// Cache idempotency key mapping for 24 hours
	r.redis.Set(ctx, "idempotency:"+key, orderID, 24*time.Hour).Err()
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

	// Configure connection pool
	config.MaxConns = 25
	config.MinConns = 5
	config.MaxConnLifetime = time.Hour
	config.MaxConnIdleTime = 30 * time.Minute

	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	// Test connection
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
