package engine

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

// ----------------------------- 配置 -----------------------------
const (
	DefaultWorkerCount    = 32
	DefaultWorkerPoolSize = 512 // 注意：不要设置得太大，避免 OOM/延迟
	DefaultMaxRetries     = 3
	DefaultProcessTimeout = 30 * time.Second
)

// ---------------------------- Metrics -----------------------------
type ConsumerMetrics struct {
	TotalProcessed int64 // 用 atomic 操作
	TotalErrors    int64
	CurrentWorkers int32
	LastError      atomic.Value // 存 string
}

func (m *ConsumerMetrics) IncProcessed()           { atomic.AddInt64(&m.TotalProcessed, 1) }
func (m *ConsumerMetrics) IncErrors()              { atomic.AddInt64(&m.TotalErrors, 1) }
func (m *ConsumerMetrics) SetLastError(err string) { m.LastError.Store(err) }
func (m *ConsumerMetrics) IncWorkers()             { atomic.AddInt32(&m.CurrentWorkers, 1) }
func (m *ConsumerMetrics) DecWorkers()             { atomic.AddInt32(&m.CurrentWorkers, -1) }

// ---------------------------- 任务与结果 -----------------------------
type MessageTask struct {
	Msg     *sarama.ConsumerMessage
	Session sarama.ConsumerGroupSession // sarama 的 session 接口可以安全并发调用 MarkMessage
}

type ProcessResult struct {
	Msg     *sarama.ConsumerMessage
	Err     error
	Session sarama.ConsumerGroupSession
}

// --------------------------- Handler ---------------------------
type ConsumerGroupHandler struct {
	workerPool     chan *MessageTask
	Metrics        *ConsumerMetrics
	wgWorkers      sync.WaitGroup
	shutdown       chan struct{}
	closed         bool     // 标记是否已关闭
	closeMutex     sync.Mutex // 保护 closed 字段
	maxRetries     int
	processTimeout time.Duration

	// 可配置的业务处理回调
	ProcessFunc func(ctx context.Context, msg *sarama.ConsumerMessage) error

	// 可选的失败处理器（当重试都失败时调用）
	OnPermanentError func(msg *sarama.ConsumerMessage, err error)
}

func NewConsumerGroupHandler(workerCount, poolSize int, maxRetries int, timeout time.Duration) *ConsumerGroupHandler {
	if workerCount <= 0 {
		workerCount = DefaultWorkerCount
	}
	if poolSize <= 0 {
		poolSize = DefaultWorkerPoolSize
	}
	if maxRetries <= 0 {
		maxRetries = DefaultMaxRetries
	}
	if timeout <= 0 {
		timeout = DefaultProcessTimeout
	}

	h := &ConsumerGroupHandler{
		workerPool:     make(chan *MessageTask, poolSize),
		Metrics:        &ConsumerMetrics{},
		shutdown:       make(chan struct{}),
		maxRetries:     maxRetries,
		processTimeout: timeout,
		ProcessFunc:    nil, // 必须由业务设置
	}
	// 初始化 LastError，避免 nil
	h.Metrics.LastError.Store("")

	// 默认永久失败处理器（可以被 SetOnPermanentError 覆盖）
	h.OnPermanentError = func(msg *sarama.ConsumerMessage, err error) {
		// 默认行为：累加错误数并记录
		h.Metrics.IncErrors()
		if err != nil {
			h.Metrics.SetLastError(err.Error())
		}
		// 这里可以把消息写死信队列 / 打日志等
		fmt.Printf("permanent error: topic=%s partition=%d offset=%d err=%v\n",
			msg.Topic, msg.Partition, msg.Offset, err)
	}

	// 启动 worker goroutine
	for i := 0; i < workerCount; i++ {
		h.wgWorkers.Add(1)
		go h.workerLoop(i)
		h.Metrics.IncWorkers()
	}

	return h
}

// Close 安全关闭：停止接收新任务，等待 worker 退出
// 可以安全地多次调用（幂等性）
func (h *ConsumerGroupHandler) Close() error {
	h.closeMutex.Lock()
	defer h.closeMutex.Unlock()

	if h.closed {
		// 已经关闭过，直接返回
		return nil
	}

	// 标记为已关闭
	h.closed = true

	// 关闭 shutdown 通道
	select {
	case <-h.shutdown:
		// already closed
	default:
		close(h.shutdown)
	}

	// 关闭任务通道让 worker 退出
	close(h.workerPool)

	// 等待所有 worker 结束
	h.wgWorkers.Wait()

	return nil
}

// SetProcessFunc 设置业务处理逻辑（必须）
func (h *ConsumerGroupHandler) SetProcessFunc(f func(ctx context.Context, msg *sarama.ConsumerMessage) error) {
	h.ProcessFunc = f
}

func (h *ConsumerGroupHandler) SetOnPermanentError(f func(msg *sarama.ConsumerMessage, err error)) {
	if f != nil {
		h.OnPermanentError = f
	}
}

// workerLoop：每个 worker 从 workerPool 获取任务，同步处理，完成后（成功）标记 offset
func (h *ConsumerGroupHandler) workerLoop(workerID int) {
	defer h.wgWorkers.Done()
	defer h.Metrics.DecWorkers()

	for task := range h.workerPool {
		// 检查是否在关闭中
		select {
		case <-h.shutdown:
			return
		default:
		}

		// 执行带重试逻辑（同步，不再额外 spawn goroutine）
		err := h.handleWithRetry(task, workerID)

		// 如果成功则 session.MarkMessage（sarama 允许从任意 goroutine 调用 MarkMessage）
		if err == nil {
			if task.Session != nil {
				task.Session.MarkMessage(task.Msg, "")
			}
			h.Metrics.IncProcessed()
		} else {
			// 所有重试失败
			h.OnPermanentError(task.Msg, err)
		}
	}
}

// handleWithRetry 包含 timeout、重试、退避 + jitter
func (h *ConsumerGroupHandler) handleWithRetry(task *MessageTask, workerID int) error {
	if h.ProcessFunc == nil {
		return errors.New("process function not set")
	}

	var lastErr error
	for attempt := 1; attempt <= h.maxRetries; attempt++ {
		// 为每次尝试创建独立的 context，以便超时控制
		ctx, cancel := context.WithTimeout(context.Background(), h.processTimeout)
		start := time.Now()
		err := safeProcessCall(h.ProcessFunc, ctx, task.Msg)
		cancel()

		if err == nil {
			// 成功
			return nil
		}

		lastErr = err
		// 记录并等待退避（带 jitter）
		// 指数退避 base * 2^(attempt-1), 加 random jitter [0, base)
		base := time.Second
		backoff := base * (1 << (attempt - 1)) // 1s,2s,4s...
		// 防止过大：限制最大退避
		maxBackoff := 10 * time.Second
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
		// jitter
		jitter := time.Duration(rand.Int63n(int64(base)))
		sleep := backoff + jitter

		// 简单日志（替换为你自己的日志）
		fmt.Printf("[worker %d] message topic=%s partition=%d offset=%d attempt=%d err=%v elapsed=%s backoff=%s\n",
			workerID, task.Msg.Topic, task.Msg.Partition, task.Msg.Offset, attempt, err, time.Since(start), sleep)

		// 如果是最后一次尝试，跳出循环直接返回最后一个错误
		if attempt == h.maxRetries {
			break
		}

		select {
		case <-time.After(sleep):
			// 继续下一次尝试
		case <-h.shutdown:
			// 如果正在 shutdown，直接返回错误
			return fmt.Errorf("shutting down, lastErr: %w", lastErr)
		}
	}

	// 所有尝试失败
	return lastErr
}

// safeProcessCall 包装业务处理，捕获 panic
func safeProcessCall(proc func(ctx context.Context, msg *sarama.ConsumerMessage) error, ctx context.Context, msg *sarama.ConsumerMessage) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()
	return proc(ctx, msg)
}

// --------------------------- sarama ConsumerGroup 接口实现 ---------------------------

func (h *ConsumerGroupHandler) Setup(session sarama.ConsumerGroupSession) error {
	// 会在新 session 开始时被调用
	fmt.Printf("Setup session: member_id=%s generation_id=%d\n", session.MemberID(), session.GenerationID())
	return nil
}

func (h *ConsumerGroupHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	// 会在 session 结束时被调用
	fmt.Printf("Cleanup session: member_id=%s\n", session.MemberID())
	return nil
}

// ConsumeClaim: 负责把消息投递到 workerPool（不再立即 MarkMessage）
// 注意：session 可能在 rebalance 时变化，但 sarama 的 MarkMessage 是线程安全的（会与当前 session 关联）
func (h *ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	fmt.Printf("ConsumeClaim start topic=%s partition=%d initial_offset=%d\n", claim.Topic(), claim.Partition(), claim.InitialOffset())

	// 使用 for range 遍历 claim.Messages()，比双重 select 更清晰
	for {
		select {
		case <-session.Context().Done():
			// session 被取消（rebalance/关闭）
			fmt.Println("session context done, exiting ConsumeClaim loop")
			return nil
		case <-h.shutdown:
			fmt.Println("handler shutdown, exiting ConsumeClaim loop")
			return nil
		case msg, ok := <-claim.Messages():
			if !ok {
				// 消息通道关闭
				fmt.Println("claim.Messages() closed")
				return nil
			}
			// 构造任务
			task := &MessageTask{
				Msg:     msg,
				Session: session,
			}

			// 投递到 workerPool（注意：这是阻塞的 — 可以背压 Kafka）
			select {
			case h.workerPool <- task:
				// 成功投递，worker 会在处理成功后调用 session.MarkMessage
			case <-h.shutdown:
				fmt.Println("shutdown during send to workerPool")
				return nil
			case <-session.Context().Done():
				return nil
			}
		}
	}
}

// --------------------------- 辅助：启动消费者组 ---------------------------

func StartKafkaConsumerGroup(brokers []string, groupID string, topics []string, handler *ConsumerGroupHandler) error {
	if handler == nil {
		return errors.New("handler is nil")
	}
	cfg := sarama.NewConfig()
	cfg.Version = sarama.MaxVersion
	cfg.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	cfg.Consumer.Offsets.Initial = sarama.OffsetNewest
	cfg.ChannelBufferSize = 4096
	cfg.Consumer.Fetch.Default = 1024 * 1024
	cfg.Consumer.MaxProcessingTime = 10 * time.Second
	cfg.Net.DialTimeout = 30 * time.Second
	cfg.Net.ReadTimeout = 30 * time.Second
	cfg.Net.WriteTimeout = 30 * time.Second

	consumerGroup, err := sarama.NewConsumerGroup(brokers, groupID, cfg)
	if err != nil {
		return fmt.Errorf("failed to create consumer group: %w", err)
	}
	defer func() {
		_ = consumerGroup.Close()
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 负责处理 sarama consumer loop
	var consumeErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if err := consumerGroup.Consume(ctx, topics, handler); err != nil {
				// 记录并短暂休眠后重试
				fmt.Printf("consumerGroup.Consume error: %v; retrying in 5s\n", err)
				consumeErr = err
				select {
				case <-time.After(5 * time.Second):
				case <-ctx.Done():
					return
				}
				continue
			}
			// Consume 返回时，检查 ctx 是否已取消
			if ctx.Err() != nil {
				return
			}
		}
	}()

	// 监控指标打印（可选）
	//metricsTicker := time.NewTicker(30 * time.Second)
	//defer metricsTicker.Stop()
	//go func() {
	//	for {
	//		select {
	//		case <-metricsTicker.C:
	//			fmt.Printf("metrics: processed=%d errors=%d active_workers=%d last_error=%v\n",
	//				atomic.LoadInt64(&handler.Metrics.TotalProcessed),
	//				atomic.LoadInt64(&handler.Metrics.TotalErrors),
	//				atomic.LoadInt32(&handler.Metrics.CurrentWorkers),
	//				handler.Metrics.LastError.Load())
	//		case <-ctx.Done():
	//			return
	//		}
	//	}
	//}()

	// 等待退出信号
	sig := waitForShutdownSignal()
	fmt.Printf("received signal: %v, shutting down\n", <-sig)
	cancel()

	// 先等待 consume loop 退出
	wg.Wait()

	// 注意：不在这里关闭 handler，由调用者负责关闭
	// 这样可以让调用者控制关闭顺序

	// 返回最早的错误（如果有）
	return consumeErr
}

func waitForShutdownSignal() <-chan os.Signal {
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	return sigchan
}
