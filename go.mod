module github.com/mjyangnb/flash-deal

go 1.22

require (
	github.com/gin-gonic/gin v1.10.0
	github.com/redis/go-redis/v9 v9.7.0
	github.com/segmentio/kafka-go v0.4.47
	github.com/jmoiron/sqlx v1.4.0
	github.com/go-sql-driver/mysql v1.8.1
	github.com/google/uuid v1.6.0
	github.com/sony/gobreaker v1.0.0
	golang.org/x/time v0.7.0
	go.opentelemetry.io/otel v1.31.0
	go.opentelemetry.io/otel/sdk v1.31.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp v1.31.0
	github.com/prometheus/client_golang v1.20.5
	github.com/spf13/viper v1.19.0
	go.uber.org/zap v1.27.0
)
