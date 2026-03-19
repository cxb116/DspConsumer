package core

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/cxb116/consumerManagers/global"
	"github.com/cxb116/consumerManagers/internal/config"
	"github.com/go-redis/redis/v8"
)

func RedisClientConnect(cfg config.Redis) (*redis.Client, error) {
	// 检查addr是否为空
	if cfg.Addr == "" {
		return nil, fmt.Errorf("redis addr cannot be empty")
	}

	opts := &redis.Options{
		Addr:     cfg.Addr,
		Username: cfg.Username,
		Password: cfg.Password,
		DB:       cfg.Db,

		// === 高并发优化配置 ===
		PoolSize:     100,            // 连接池大小：根据并发量调整，建议 50-200
		MinIdleConns: 20,             // 最小空闲连接数：保持预热连接，建议 10-30
		MaxRetries:   3,              // 最大重试次数：网络抖动时重试
		MaxRetryBackoff: 512 * time.Millisecond, // 最大重试退避时间
		MinRetryBackoff: 8 * time.Millisecond,  // 最小重试退避时间

		// === 超时配置 ===
		DialTimeout:  5 * time.Second,  // 连接超时
		ReadTimeout:  3 * time.Second,  // 读取超时（0 表示无限制）
		WriteTimeout: 3 * time.Second,  // 写入超时（0 表示无限制）
		PoolTimeout:  4 * time.Second,  // 从连接池获取连接的超时时间

		// === 连接生命周期管理 ===
		IdleTimeout:     5 * time.Minute, // 空闲连接超时时间，自动清理
		IdleCheckFrequency: 1 * time.Minute, // 空闲连接检查频率
		MaxConnAge:      30 * time.Minute, // 连接最大使用时间，避免长连接问题
	}
	redisClient := redis.NewClient(opts)

	// 正确检查Ping结果
	result, err := redisClient.Ping(context.Background()).Result()
	if err != nil {
		log.Printf("redis ping failed: %v", err)
		return nil, fmt.Errorf("redis ping failed: %w", err)
	}

	log.Printf("redis connect success, ping result: %s, addr: %s", result, cfg.Addr)
	return redisClient, nil
}

func NewClientRedis() *redis.Client {
	client, err := RedisClientConnect(global.EngineConfig.Redis)

	if err != nil {
		log.Println("redis connect fail")
		panic(err)
		return nil
	}
	log.Println("redis connect success")
	return client
}
