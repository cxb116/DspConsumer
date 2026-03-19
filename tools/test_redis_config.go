package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/go-redis/redis/v8"
)

func main() {
	fmt.Println("=== Redis 高并发配置验证 ===")
	fmt.Println()

	// Redis 配置（与你的项目配置相同）
	opts := &redis.Options{
		Addr:     "127.0.0.1:6379",
		Username: "root",
		Password: "",
		DB:       0,

		// === 高并发优化配置 ===
		PoolSize:           100,
		MinIdleConns:       20,
		MaxRetries:         3,
		MaxRetryBackoff:    512 * time.Millisecond,
		MinRetryBackoff:    8 * time.Millisecond,
		DialTimeout:        5 * time.Second,
		ReadTimeout:        3 * time.Second,
		WriteTimeout:       3 * time.Second,
		PoolTimeout:        4 * time.Second,
		IdleTimeout:        5 * time.Minute,
		IdleCheckFrequency: 1 * time.Minute,
		MaxConnAge:         30 * time.Minute,
	}

	// 创建 Redis 客户端
	client := redis.NewClient(opts)
	defer client.Close()

	// 测试连接
	ctx := context.Background()
	result, err := client.Ping(ctx).Result()
	if err != nil {
		log.Fatalf("❌ Redis 连接失败: %v\n", err)
	}

	fmt.Println("✅ Redis 连接成功!")
	fmt.Printf("   Ping 响应: %s\n", result)
	fmt.Println()

	// 显示配置信息
	fmt.Println("📊 配置信息:")
	fmt.Printf("   连接池大小 (PoolSize):      %d\n", opts.PoolSize)
	fmt.Printf("   最小空闲连接 (MinIdleConns): %d\n", opts.MinIdleConns)
	fmt.Printf("   最大重试次数 (MaxRetries):   %d\n", opts.MaxRetries)
	fmt.Printf("   连接超时 (DialTimeout):      %v\n", opts.DialTimeout)
	fmt.Printf("   读取超时 (ReadTimeout):      %v\n", opts.ReadTimeout)
	fmt.Printf("   写入超时 (WriteTimeout):     %v\n", opts.WriteTimeout)
	fmt.Println()

	// 测试基本操作
	fmt.Println("🧪 测试基本操作:")

	// SET 操作
	start := time.Now()
	err = client.Set(ctx, "test_key", "test_value", 10*time.Minute).Err()
	if err != nil {
		log.Printf("❌ SET 失败: %v\n", err)
	} else {
		fmt.Printf("✅ SET 操作成功 (耗时: %v)\n", time.Since(start))
	}

	// GET 操作
	start = time.Now()
	val, err := client.Get(ctx, "test_key").Result()
	if err != nil {
		log.Printf("❌ GET 失败: %v\n", err)
	} else {
		fmt.Printf("✅ GET 操作成功: %s (耗时: %v)\n", val, time.Since(start))
	}

	// 测试 Pipeline
	fmt.Println()
	fmt.Println("🚀 测试 Pipeline 批量操作:")
	start = time.Now()

	pipe := client.Pipeline()
	for i := 0; i < 10; i++ {
		pipe.Set(ctx, fmt.Sprintf("pipeline_key_%d", i), fmt.Sprintf("value_%d", i), 5*time.Minute)
	}
	cmds, err := pipe.Exec(ctx)
	if err != nil {
		log.Printf("❌ Pipeline 执行失败: %v\n", err)
	} else {
		fmt.Printf("✅ Pipeline 批量操作成功: %d 个命令 (耗时: %v)\n", len(cmds), time.Since(start))
	}

	// 查看连接池状态
	fmt.Println()
	stats := client.PoolStats()
	fmt.Println("📈 连接池状态:")
	fmt.Printf("   命中次数 (Hits):      %d\n", stats.Hits)
	fmt.Printf("   未命中次数 (Misses):  %d\n", stats.Misses)
	fmt.Printf("   超时次数 (Timeouts):  %d\n", stats.Timeouts)

	if stats.Hits+stats.Misses > 0 {
		hitRate := float64(stats.Hits) / float64(stats.Hits+stats.Misses) * 100
		fmt.Printf("   连接池命中率:        %.2f%%\n", hitRate)
	}

	fmt.Println()
	fmt.Println("=== 验证完成 ===")
}
