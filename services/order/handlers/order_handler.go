package handlers

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/orderhub/services/order/service"
)

type OrderHandler struct {
	orderService *service.OrderService
	logger       *zap.Logger
}

func NewOrderHandler(orderService *service.OrderService, logger *zap.Logger) *OrderHandler {
	return &OrderHandler{
		orderService: orderService,
		logger:       logger,
	}
}

// CreateOrderRequest represents the request body for creating an order
type CreateOrderRequest struct {
	UserID    string           `json:"user_id" binding:"required"`
	Items     []OrderItemInput `json:"items" binding:"required,min=1"`
	Shipping  ShippingAddress  `json:"shipping" binding:"required"`
	IdempotencyKey string      `json:"idempotency_key,omitempty"`
}

type OrderItemInput struct {
	ProductID string `json:"product_id" binding:"required"`
	Quantity  int    `json:"quantity" binding:"required,min=1"`
	Price     int64  `json:"price" binding:"required,min=0"`
}

type ShippingAddress struct {
	Street  string `json:"street" binding:"required"`
	City    string `json:"city" binding:"required"`
	State   string `json:"state" binding:"required"`
	ZipCode string `json:"zip_code" binding:"required"`
	Country string `json:"country" binding:"required"`
}

// CreateOrder handles POST /api/v1/orders
func (h *OrderHandler) CreateOrder(c *gin.Context) {
	var req CreateOrderRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		h.logger.Warn("Invalid order request", zap.Error(err))
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request", "details": err.Error()})
		return
	}

	// Set idempotency key from header if not in body
	if req.IdempotencyKey == "" {
		req.IdempotencyKey = c.GetHeader("X-Idempotency-Key")
	}

	ctx := c.Request.Context()
	order, err := h.orderService.CreateOrder(ctx, service.CreateOrderCommand{
		UserID:         req.UserID,
		Items:          convertToOrderItems(req.Items),
		Shipping:       req.Shipping,
		IdempotencyKey: req.IdempotencyKey,
	})
	if err != nil {
		h.logger.Error("Failed to create order", zap.Error(err))
		
		switch err {
		case service.ErrDuplicateOrder:
			c.JSON(http.StatusConflict, gin.H{"error": "Duplicate order", "message": "Order with this idempotency key already exists"})
			return
		case service.ErrInsufficientInventory:
			c.JSON(http.StatusBadRequest, gin.H{"error": "Insufficient inventory", "message": "One or more items are out of stock"})
			return
		case service.ErrPaymentFailed:
			c.JSON(http.StatusPaymentRequired, gin.H{"error": "Payment failed", "message": "Unable to process payment"})
			return
		default:
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create order", "message": err.Error()})
			return
		}
	}

	c.JSON(http.StatusCreated, gin.H{
		"order_id": order.ID,
		"status":   order.Status,
		"total":    order.TotalAmount,
		"created_at": order.CreatedAt,
	})
}

// GetOrder handles GET /api/v1/orders/:id
func (h *OrderHandler) GetOrder(c *gin.Context) {
	orderID := c.Param("id")
	if orderID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Order ID is required"})
		return
	}

	ctx := c.Request.Context()
	order, err := h.orderService.GetOrder(ctx, orderID)
	if err != nil {
		if err == service.ErrOrderNotFound {
			c.JSON(http.StatusNotFound, gin.H{"error": "Order not found"})
			return
		}
		h.logger.Error("Failed to get order", zap.String("order_id", orderID), zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to retrieve order"})
		return
	}

	c.JSON(http.StatusOK, formatOrderResponse(order))
}

// ListOrders handles GET /api/v1/orders
func (h *OrderHandler) ListOrders(c *gin.Context) {
	ctx := c.Request.Context()

	// Parse pagination parameters
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	pageSize, _ := strconv.Atoi(c.DefaultQuery("page_size", "20"))
	status := c.Query("status")
	userID := c.Query("user_id")

	if page < 1 {
		page = 1
	}
	if pageSize < 1 || pageSize > 100 {
		pageSize = 20
	}

	orders, total, err := h.orderService.ListOrders(ctx, service.ListOrdersQuery{
		UserID: userID,
		Status: status,
		Page:   page,
		Limit:  pageSize,
	})
	if err != nil {
		h.logger.Error("Failed to list orders", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to retrieve orders"})
		return
	}

	response := make([]gin.H, len(orders))
	for i, order := range orders {
		response[i] = formatOrderResponse(order)
	}

	c.JSON(http.StatusOK, gin.H{
		"orders":     response,
		"total":      total,
		"page":       page,
		"page_size":  pageSize,
		"total_pages": (total + int64(pageSize) - 1) / int64(pageSize),
	})
}

func convertToOrderItems(items []OrderItemInput) []service.OrderItem {
	result := make([]service.OrderItem, len(items))
	for i, item := range items {
		result[i] = service.OrderItem{
			ProductID: item.ProductID,
			Quantity:  item.Quantity,
			Price:     item.Price,
		}
	}
	return result
}

func formatOrderResponse(order *service.Order) gin.H {
	items := make([]gin.H, len(order.Items))
	for i, item := range order.Items {
		items[i] = gin.H{
			"product_id": item.ProductID,
			"quantity":   item.Quantity,
			"price":      item.Price,
		}
	}

	return gin.H{
		"id":           order.ID,
		"user_id":      order.UserID,
		"status":       order.Status,
		"items":        items,
		"shipping":     order.Shipping,
		"total_amount": order.TotalAmount,
		"created_at":   order.CreatedAt,
		"updated_at":   order.UpdatedAt,
	}
}
