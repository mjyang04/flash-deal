package config_test

import (
	"os"
	"testing"

	"github.com/mjyangnb/flash-deal/internal/config"
)

func TestLoad_Defaults(t *testing.T) {
	// 清掉可能干扰的 env
	os.Unsetenv("FD_HTTP_ADDR")
	os.Unsetenv("FD_MYSQL_DSN")
	os.Unsetenv("FD_REDIS_ADDR")

	cfg, err := config.Load("")
	if err != nil {
		t.Fatalf("Load default: %v", err)
	}
	if cfg.HTTP.Addr != ":8080" {
		t.Errorf("HTTP.Addr = %q, want :8080", cfg.HTTP.Addr)
	}
	if cfg.MySQL.MaxOpenConns != 100 {
		t.Errorf("MySQL.MaxOpenConns = %d, want 100", cfg.MySQL.MaxOpenConns)
	}
	if cfg.Redis.Addr != "127.0.0.1:6380" {
		t.Errorf("Redis.Addr = %q, want 127.0.0.1:6380", cfg.Redis.Addr)
	}
}

func TestLoad_EnvOverride(t *testing.T) {
	t.Setenv("FD_HTTP_ADDR", ":9090")
	t.Setenv("FD_MYSQL_DSN", "user:pw@tcp(db:3306)/x")

	cfg, err := config.Load("")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.HTTP.Addr != ":9090" {
		t.Errorf("HTTP.Addr = %q, want :9090", cfg.HTTP.Addr)
	}
	if cfg.MySQL.DSN != "user:pw@tcp(db:3306)/x" {
		t.Errorf("DSN override missed: %q", cfg.MySQL.DSN)
	}
}

func TestLoad_KafkaDefaults(t *testing.T) {
	cfg, err := config.Load("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Kafka.OrderTopic != "seckill_orders" {
		t.Errorf("OrderTopic = %q", cfg.Kafka.OrderTopic)
	}
	if len(cfg.Kafka.Brokers) != 1 || cfg.Kafka.Brokers[0] != "127.0.0.1:9092" {
		t.Errorf("Brokers = %v", cfg.Kafka.Brokers)
	}
	if !cfg.Switches.LuaStock || !cfg.Switches.KafkaOrder {
		t.Errorf("Switches not on by default: %+v", cfg.Switches)
	}
}

func TestLoad_SwitchesOff(t *testing.T) {
	t.Setenv("FD_SWITCHES_LUA_STOCK", "false")
	t.Setenv("FD_SWITCHES_KAFKA_ORDER", "false")
	cfg, _ := config.Load("")
	if cfg.Switches.LuaStock || cfg.Switches.KafkaOrder {
		t.Errorf("switches did not flip: %+v", cfg.Switches)
	}
}
