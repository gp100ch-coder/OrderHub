module github.com/orderhub/services/order

go 1.21

require (
	github.com/gin-gonic/gin v1.9.1
	github.com/confluentinc/confluent-kafka-go/v2 v2.3.0
	github.com/redis/go-redis/v9 v9.3.0
	github.com/jackc/pgx/v5 v5.5.0
	github.com/sony/gobreaker v0.5.0
	go.uber.org/zap v1.26.0
	github.com/prometheus/client_golang v1.18.0
	github.com/grpc-ecosystem/go-grpc-middleware v1.4.0
	google.golang.org/grpc v1.60.0
	google.golang.org/protobuf v1.32.0
	github.com/spf13/viper v1.18.0
	github.com/golang-migrate/migrate/v4 v4.17.0
	github.com/stretchr/testify v1.8.4
	github.com/orderhub/proto v0.0.0
	github.com/orderhub/shared v0.0.0
)

replace (
	github.com/orderhub/proto => ../../proto
	github.com/orderhub/shared => ../../shared
)
