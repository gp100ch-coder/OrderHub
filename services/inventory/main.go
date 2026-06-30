package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/orderhub/proto/pb"
	"github.com/orderhub/services/inventory/service"
	"github.com/orderhub/services/inventory/repository"
	"github.com/orderhub/shared/kafka"
	"github.com/orderhub/shared/logger"
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

	log.Info("Starting Inventory Service",
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

	// Initialize inventory repository
	inventoryRepo := repository.NewInventoryRepository(db, redisClient, log)

	// Initialize inventory service
	inventoryService := service.NewInventoryService(inventoryRepo, log)

	// Start gRPC server
	go func() {
		grpcAddr := fmt.Sprintf(":%d", viper.GetInt("grpc.port"))
		lis, err := net.Listen("tcp", grpcAddr)
		if err != nil {
			log.Fatal("Failed to listen on gRPC port", zap.Error(err))
		}

		grpcServer := grpc.NewServer()
		pb.RegisterInventoryServiceServer(grpcServer, inventoryService)

		log.Info("gRPC server starting", zap.String("address", grpcAddr))
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatal("gRPC server failed", zap.Error(err))
		}
	}()

	// Initialize Kafka consumer for order events
	kafkaConsumer, err := kafka.NewConsumer(
		[]string{viper.GetString("kafka.brokers")},
		viper.GetString("kafka.consumer_group"),
		[]string{"order-events"},
	)
	if err != nil {
		log.Fatal("Failed to create Kafka consumer", zap.Error(err))
	}
	defer kafkaConsumer.Close()

	// Start consuming events
	go func() {
		ctx := context.Background()
		for {
			msg, err := kafkaConsumer.Consume(ctx)
			if err != nil {
				log.Error("Failed to consume message", zap.Error(err))
				time.Sleep(time.Second)
				continue
			}

			if err := inventoryService.HandleOrderEvent(ctx, msg); err != nil {
				log.Error("Failed to handle order event", zap.Error(err))
				// Could send to dead-letter queue here
			}
		}
	}()

	// Setup HTTP server for health and metrics
	router := gin.New()
	router.Use(gin.Recovery())

	// Health check endpoint
	router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "healthy"})
	})

	// Prometheus metrics endpoint
	router.GET("/metrics", gin.WrapH(promhttp.Handler()))

	httpAddr := fmt.Sprintf(":%d", viper.GetInt("server.port"))
	srv := &http.Server{
		Addr:         httpAddr,
		Handler:      router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		log.Info("HTTP server starting", zap.String("address", httpAddr))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal("HTTP server failed", zap.Error(err))
		}
	}()

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Info("Shutting down Inventory Service...")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Fatal("HTTP server forced to shutdown", zap.Error(err))
	}

	log.Info("Inventory Service stopped")
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
	viper.SetDefault("server.port", 8081)
	viper.SetDefault("grpc.port", 9001)
	viper.SetDefault("db.port", 5432)
	viper.SetDefault("redis.port", 6379)
	viper.SetDefault("kafka.brokers", "localhost:9092")
	viper.SetDefault("kafka.consumer_group", "inventory-service")

	if err := viper.ReadInConfig(); err != nil {
		fmt.Println("No config file found, using defaults and environment variables")
	}

	viper.AutomaticEnv()
	return nil
}
