package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/cxb116/consumerManagers/engine"
	"github.com/cxb116/consumerManagers/global"
	"github.com/cxb116/consumerManagers/implement"
	"github.com/cxb116/consumerManagers/internal/core"
	"github.com/cxb116/consumerManagers/internal/logger"
)

// formatMessage 格式化 Kafka 消息
// req/res topic 添加前缀，上报消息直接使用原始内容
func formatMessage(topic, value string) string {

	logger.SystemLog.Log().Msgf("topic: %s, value: %s", topic, value)
	if topic == "res" {
		return topic + ":" + value
	} else if topic == "ims" {
		return topic + ":" + value
	} else if topic == "clk" {
		return topic + ":" + value
	}
	return value
}

func main() {
	// 初始化配置和连接
	global.EngineViper = core.Viper()
	global.EngineDB = core.NewClientMysql()
	global.EngineRedis = core.NewClientRedis()

	// Kafka 配置
	brokers := global.EngineConfig.Kafka.Brokers
	groupID := global.EngineConfig.Kafka.GroupID
	topics := global.EngineConfig.Kafka.Topics

	// 建立 handler：32 worker, 512 pool, 3 retries, 30s timeout
	handler := engine.NewConsumerGroupHandler(32, 512, 3, 30*time.Second)

	// 注入业务处理逻辑
	handler.SetProcessFunc(func(ctx context.Context, msg *sarama.ConsumerMessage) error {
		// 使用 ctx 检查超时 / 取消
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		logger.SystemLog.Info().Msgf("Message: %s", string(msg.Value))
		// 格式化消息：req/res topic 添加前缀，上报消息直接使用
		formattedMsg := formatMessage(msg.Topic, string(msg.Value))

		// 发送到任务队列处理
		implement.GlobalWorkerPool.SendToTaskQueue(formattedMsg)

		return nil
	})

	// 可选：自定义永久失败回调
	//handler.SetOnPermanentError(func(msg *sarama.ConsumerMessage, err error) {
	//	handler.Metrics.IncErrors()
	//	if err != nil {
	//		handler.Metrics.SetLastError(err.Error())
	//	}
	//	// 将失败消息写入死信队列或持久化
	//	fmt.Printf("PERMANENT FAIL: topic=%s partition=%d offset=%d err=%v\n",
	//		msg.Topic, msg.Partition, msg.Offset, err)
	//})

	// 在 goroutine 中启动消费者，这样可以在主 goroutine 中监听信号
	consumerErr := make(chan error, 1)
	go func() {
		if err := engine.StartKafkaConsumerGroup(brokers, groupID, topics, handler); err != nil {
			consumerErr <- fmt.Errorf("consumer group stopped with error: %w", err)
		} else {
			consumerErr <- nil
		}
	}()

	// 监听关闭信号
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	// 等待信号
	sig := <-sigchan
	logger.SystemLog.Info().Msgf("Received signal: %v, shutting down gracefully...", sig)

	// 优雅关闭流程
	shutdownStart := time.Now()

	// 1. 关闭 WorkerPool (停止接收新任务)
	logger.SystemLog.Info().Msgf("Step 1: Closing WorkerPool...")
	implement.GlobalWorkerPool.Close()
	logger.SystemLog.Info().Msgf("WorkerPool closed (took: %v)", time.Since(shutdownStart))

	// 2. 关闭 Kafka Handler (等待 worker 完成)
	logger.SystemLog.Info().Msgf("Step 2: Closing Kafka Handler...")
	if err := handler.Close(); err != nil {
		logger.SystemLog.Error().Msgf("Handler close error: %v", err)
	}
	logger.SystemLog.Info().Msgf("Kafka Handler closed (took: %v)", time.Since(shutdownStart))

	// 3. 等待消费者停止
	logger.SystemLog.Info().Msgf("Step 3: Waiting for consumer to stop...")
	select {
	case err := <-consumerErr:
		if err != nil {
			logger.SystemLog.Error().Msgf("Consumer error: %v", err)
		} else {
			logger.SystemLog.Info().Msgf("Consumer stopped cleanly")
		}
	case <-time.After(30 * time.Second):
		logger.SystemLog.Warn().Msgf("Consumer shutdown timeout after 30s")
	}
	logger.SystemLog.Info().Msgf("Consumer stopped (took: %v)", time.Since(shutdownStart))

	// 4. 关闭 Redis 连接
	logger.SystemLog.Info().Msgf("Step 4: Closing Redis connection...")
	if global.EngineRedis != nil {
		if err := global.EngineRedis.Close(); err != nil {
			logger.SystemLog.Error().Msgf("Redis close error: %v", err)
		} else {
			logger.SystemLog.Info().Msgf("Redis connection closed")
		}
	}

	// 5. 关闭 MySQL 连接
	logger.SystemLog.Info().Msgf("Step 5: Closing MySQL connection...")
	sqlDB, err := global.EngineDB.DB()
	if err != nil {
		logger.SystemLog.Error().Msgf("Get SQL DB error: %v", err)
	} else {
		if err := sqlDB.Close(); err != nil {
			logger.SystemLog.Error().Msgf("MySQL close error: %v", err)
		} else {
			logger.SystemLog.Info().Msgf("MySQL connection closed")
		}
	}

	totalShutdownTime := time.Since(shutdownStart)
	logger.SystemLog.Info().Msgf("==========================================")
	logger.SystemLog.Info().Msgf("Graceful shutdown completed!")
	logger.SystemLog.Info().Msgf("Total shutdown time: %v", totalShutdownTime)
	logger.SystemLog.Info().Msgf("==========================================")
}
