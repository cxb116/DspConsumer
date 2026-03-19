package implement

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cxb116/consumerManagers/internal/logger"
)

const (
	WORK_POOL_SIZE      = 2000 // 池大小
	MAXWORKER_QUEUE_LEN = 2000
)

// min 返回两个整数中的较小值
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

var GlobalWorkerPool *WorkerPool

func init() {
	GlobalWorkerPool = NewWorkerPool()
	GlobalWorkerPool.StartWorkerPool()
}

// truncateString 截断字符串到指定长度
func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}

type WorkerPool struct {
	WorkerPoolLen     int
	MaxWorkerQueueLen int
	TaskQueue         []chan string
	roundRobinCounter uint64     // 用于轮询分配的原子计数器
	closed            bool       // 标记是否已关闭
	closeMutex        sync.Mutex // 保护 closed 字段和关闭操作
}

func NewWorkerPool() *WorkerPool {
	return &WorkerPool{
		WorkerPoolLen:     WORK_POOL_SIZE,
		MaxWorkerQueueLen: MAXWORKER_QUEUE_LEN,
		TaskQueue:         make([]chan string, WORK_POOL_SIZE),
	}
}

func (this *WorkerPool) StartWorkerPool() {
	for i := 0; i < this.WorkerPoolLen; i++ {
		this.TaskQueue[i] = make(chan string, this.MaxWorkerQueueLen)
		go this.StarOnWorker(i, this.TaskQueue[i])
	}
	logger.SystemLog.Info().Msgf("WorkerPool started with %d workers, queue size: %d", this.WorkerPoolLen, this.MaxWorkerQueueLen)
}

func (this *WorkerPool) StarOnWorker(workerId int, contexts chan string) {
	for {
		select {
		case consumerContex, ok := <-contexts:
			if !ok {
				// channel 已关闭，退出 goroutine
				logger.SystemLog.Warn().Msgf("worker %d channel closed, exiting", workerId)
				return
			}

			// 处理消息
			this.doMessageDisPatcher(consumerContex, workerId)
		}
	}
}

// SendToTaskQueue 使用 Round-Robin 方式发送消息到任务队列
// 尝试所有 worker，如果都满了则丢弃消息并记录错误
func (this *WorkerPool) SendToTaskQueue(ctx string) {
	// 检查是否已关闭
	this.closeMutex.Lock()
	closed := this.closed
	this.closeMutex.Unlock()

	if closed {
		logger.SystemLog.Warn().Msgf("[WorkerPool] Attempted to send message after pool closed")
		return
	}

	// 尝试最多 WORK_POOL_SIZE 次，找到有空闲位置的 worker
	for i := 0; i < this.WorkerPoolLen; i++ {
		workerId := int(atomic.AddUint64(&this.roundRobinCounter, 1)) % this.WorkerPoolLen

		select {
		case this.TaskQueue[workerId] <- ctx:
			// 发送成功
			return
		default:
			// channel 已满，尝试下一个 worker
			continue
		}
	}

	// 所有 worker 都满了，记录错误并丢弃消息
	logger.SystemLog.Error().Msgf("[WorkerPool] All workers busy, message dropped (queue size: %d)", this.MaxWorkerQueueLen)
}

// Close 优雅关闭 WorkerPool
// 可以安全地多次调用（幂等性）
func (this *WorkerPool) Close() {
	this.closeMutex.Lock()
	defer this.closeMutex.Unlock()

	if this.closed {
		// 已经关闭过，直接返回
		return
	}

	// 标记为已关闭
	this.closed = true

	logger.SystemLog.Info().Msgf("Closing WorkerPool...")
	for i, ch := range this.TaskQueue {
		close(ch)
		logger.SystemLog.Debug().Msgf("worker %d channel closed", i)
	}
	logger.SystemLog.Info().Msgf("WorkerPool closed successfully")
}

func (this *WorkerPool) doMessageDisPatcher(ctxMsg string, workerId int) {
	defer func() {
		if err := recover(); err != nil {
			logger.ErrorLog.Error().
				Msgf("[dispatcher panic] workerId=%d err=%v", workerId, err)
		}
	}()

	// 检查消息是否为空
	if ctxMsg == "" {
		logger.ErrorLog.Warn().Msgf("workerId=%d, 收到空消息", workerId)
		return
	}

	logger.SystemLog.Info().Msgf("workerId: %s", ctxMsg)
	// 判断消息类型并处理
	// 创建带超时的context，避免Redis操作卡住
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if strings.HasPrefix(ctxMsg, "res:") {
		// Kafka 消息（res topic）：移除前缀后处理
		msgData := ctxMsg[4:] // 移除 "res:" 前缀
		if err := HandleKafkaTimeWindow(ctx, msgData); err != nil {
			logger.ErrorLog.Error().Msgf("workerId=%d, HandleKafkaTimeWindow error: %v, msg: %s", workerId, err, msgData)
		}
	} else if strings.HasPrefix(ctxMsg, "ims:") {
		// 展现消息（ims）：移除前缀后处理
		msgData := ctxMsg[4:] // 移除 "ims:" 前缀
		if err := HandleImpressionMetrics(ctx, msgData); err != nil {
			logger.ErrorLog.Error().Msgf("workerId=%d, HandleImpressionMetrics error: %v, msg: %s", workerId, err, msgData)
		}
	} else if strings.HasPrefix(ctxMsg, "clk:") {
		// 点击消息（clk）：移除前缀后处理
		msgData := ctxMsg[4:] // 移除 "clk:" 前缀
		if err := HandleClickMetrics(ctx, msgData); err != nil {
			logger.ErrorLog.Error().Msgf("workerId=%d, HandleClickMetrics error: %v, msg: %s", workerId, err, msgData)
		}
	} else {
		// 其他类型的消息（例如上报消息）
		logger.ErrorLog.Info().Msgf("workerId=%d, 未处理的消息类型: %s", workerId, ctxMsg[:min(len(ctxMsg), 100)])
	}

}
