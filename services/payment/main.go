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
	"github.com/orderhub/services/payment/service"
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

	log.Info("Starting Payment Service",
		zap.String("version", viper.GetString("app.version")),
		zap.String("env", viper.GetString("app.env")))

	// Initialize payment service
	paymentService := service.NewPaymentService(log)

	// Start gRPC server
	grpcAddr := fmt.Sprintf(":%d", viper.GetInt("grpc.port"))
	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		log.Fatal("Failed to listen on gRPC port", zap.Error(err))
	}

	grpcServer := grpc.NewServer()
	pb.RegisterPaymentServiceServer(grpcServer, paymentService)

	go func() {
		log.Info("gRPC server starting", zap.String("address", grpcAddr))
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatal("gRPC server failed", zap.Error(err))
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

	log.Info("Shutting down Payment Service...")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Fatal("HTTP server forced to shutdown", zap.Error(err))
	}

	log.Info("Payment Service stopped")
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
	viper.SetDefault("server.port", 8082)
	viper.SetDefault("grpc.port", 9002)

	if err := viper.ReadInConfig(); err != nil {
		fmt.Println("No config file found, using defaults and environment variables")
	}

	viper.AutomaticEnv()
	return nil
}
