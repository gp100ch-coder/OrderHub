package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/orderhub/services/order/handlers"
	"github.com/orderhub/services/order/repository"
	"github.com/orderhub/services/order/service"
	"github.com/orderhub/shared/logger"
	"github.com/orderhub/shared/kafka"
)

func main() {
	// Load configuration
	if err := loadConfig(); err != nil {
		panic(fmt.Sprintf("Failed to load config: %v", err))
	}

	// Initialize logger
	log, err := logger.New(viper.GetString("log.level"))
	if err != nil {
		panic(fmt.Sprintf("Failed to initialize logger: %v", err))
	}
	defer log.Sync()

	log.Info("Starting Order Service",
		zap.String("version", viper.GetString("app.version")),
		zap.String("env", viper.GetString("app.env")))

	// Initialize database connection
	db, err := repository.NewPostgresDB(
		viper.GetString("db.host"),
		viper.GetInt("db.port"),
		viper.GetString("db.name"),
		viper.GetString("db.user"),
		viper.GetString("db.password"),
	)
	if err != nil {
		log.Fatal("Failed to connect to database", zap.Error(err))
	}
	defer db.Close()

	// Initialize Redis client
	redisClient := repository.NewRedisClient(
		viper.GetString("redis.host"),
		viper.GetInt("redis.port"),
		viper.GetString("redis.password"),
	)
	defer redisClient.Close()

	// Initialize Kafka producer
	kafkaProducer, err := kafka.NewProducer([]string{viper.GetString("kafka.brokers")})
	if err != nil {
		log.Fatal("Failed to create Kafka producer", zap.Error(err))
	}
	defer kafkaProducer.Close()

	// Initialize order repository
	orderRepo := repository.NewOrderRepository(db, log)

	// Initialize order service with saga orchestrator
	orderService := service.NewOrderService(
		orderRepo,
		redisClient,
		kafkaProducer,
		log,
		viper.GetString("inventory.service.address"),
		viper.GetString("payment.service.address"),
	)

	// Initialize handlers
	orderHandler := handlers.NewOrderHandler(orderService, log)

	// Setup Gin router
	router := gin.New()
	router.Use(gin.Recovery())
	router.Use(loggerMiddleware(log))

	// Health check endpoint
	router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "healthy"})
	})

	// Prometheus metrics endpoint
	router.GET("/metrics", gin.WrapH(promhttp.Handler()))

	// Order routes
	orderGroup := router.Group("/api/v1/orders")
	{
		orderGroup.POST("", orderHandler.CreateOrder)
		orderGroup.GET("/:id", orderHandler.GetOrder)
		orderGroup.GET("", orderHandler.ListOrders)
	}

	// Create HTTP server
	srv := &http.Server{
		Addr:         fmt.Sprintf(":%d", viper.GetInt("server.port")),
		Handler:      router,
		ReadTimeout:  viper.GetDuration("server.read_timeout"),
		WriteTimeout: viper.GetDuration("server.write_timeout"),
		IdleTimeout:  viper.GetDuration("server.idle_timeout"),
	}

	// Start server in goroutine
	go func() {
		log.Info("HTTP server starting", zap.Int("port", viper.GetInt("server.port")))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal("HTTP server failed", zap.Error(err))
		}
	}()

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Info("Shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Fatal("Server forced to shutdown", zap.Error(err))
	}

	log.Info("Order Service stopped")
}

func loadConfig() error {
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")
	viper.AddConfigPath("./config")
	viper.AddConfigPath("/etc/orderhub/")

	// Set defaults
	viper.SetDefault("app.version", "1.0.0")
	viper.SetDefault("app.env", "development")
	viper.SetDefault("log.level", "info")
	viper.SetDefault("server.port", 8080)
	viper.SetDefault("server.read_timeout", 15*time.Second)
	viper.SetDefault("server.write_timeout", 15*time.Second)
	viper.SetDefault("server.idle_timeout", 60*time.Second)
	viper.SetDefault("db.port", 5432)
	viper.SetDefault("redis.port", 6379)
	viper.SetDefault("kafka.brokers", "localhost:9092")

	if err := viper.ReadInConfig(); err != nil {
		// Config file is optional, continue with defaults and env vars
		fmt.Println("No config file found, using defaults and environment variables")
	}

	viper.AutomaticEnv()
	return nil
}

func loggerMiddleware(log *zap.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path

		c.Next()

		latency := time.Since(start)
		statusCode := c.Writer.Status()

		log.Info("HTTP request",
			zap.String("method", c.Request.Method),
			zap.String("path", path),
			zap.Int("status", statusCode),
			zap.Duration("latency", latency),
			zap.String("client_ip", c.ClientIP()),
			zap.String("request_id", c.GetString("request_id")),
		)
	}
}
