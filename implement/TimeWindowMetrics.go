package implement

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/cxb116/consumerManagers/global"
	"github.com/cxb116/consumerManagers/internal/logger"
)

// DspRequestMetrics DSP请求指标数据结构

// HandleKafkaTimeWindow 处理 Kafka 消息的时间窗统计（主入口函数）
func HandleKafkaTimeWindow(ctx context.Context, msgStr string) error {
	// 1. 解析JSON消息
	var metrics DspRequestMetrics
	if err := json.Unmarshal([]byte(msgStr), &metrics); err != nil {
		//logger.SystemLog.Error().Msgf("解析DSP指标消息,JSON转换失败: %v, msg: %s", err, msgStr)
		logger.SystemLog.Error().Msgf("json.Unmarshal error: %v", err)
		return err
	}

	// 2. 数据校验
	if metrics.SspSlotId == 0 {
		logger.SystemLog.Warn().Msgf("SspSlotId为空，跳过处理: %s", metrics.RequestId)
		return fmt.Errorf("SspSlotId不能为空")
	}

	if metrics.EventTs == 0 {
		logger.SystemLog.Warn().Msgf("EventTs为空，使用当前时间: %s", metrics.RequestId)
		metrics.EventTs = getCurrentEventTs()
	}

	// 3. 计算时间窗（向上对齐到10分钟）
	// 例如: 202602252050 → 202602252100
	eventTsStr := strconv.FormatInt(metrics.EventTs, 10)
	timeWindowKey := alignToNext10Minutes(eventTsStr)

	// 4. 构建Redis key    key = dsp:202602252100
	redisKey := buildRedisKey(global.EngineConfig.HashKey, timeWindowKey) // dsp:2026030701420

	// 5. 处理各项统计指标（8种field类型）
	if err := processAllMetrics(ctx, redisKey, &metrics); err != nil {
		logger.SystemLog.Error().Msgf("处理DSP指标失败: %v, key: %s, RequestId: %s",
			err, redisKey, metrics.RequestId)
		return err
	}

	logger.SystemLog.Info().Msgf(
		"DSP指标处理成功 - key: %s, SspSlotId: %d, DspSlotId: %d, RequestId: %s",
		redisKey, metrics.SspSlotId, metrics.DspSlotId, metrics.RequestId,
	)

	return nil
}

// processAllMetrics 处理所有统计指标（8种field类型）
// Field格式:
//   正常类（4种）:
//     req:ssp:dspSlotId:dspSlotCode:sspSlotId      → 媒体请求
//     req:dsp:dspSlotId:dspSlotCode:sspSlotId       → 预算请求
//     res:ssp:dspSlotId:dspSlotCode:sspSlotId       → 媒体响应
//     res:dsp:dspSlotId:dspSlotCode:sspSlotId       → 预算响应
//
//   丢弃类（4种）: TODO: 丢弃先不管
//     req:discard:ssp:dspSlotId:dspSlotCode:sspSlotId    → 媒体请求过滤
//     req:discard:dsp:dspSlotId:dspSlotCode:sspSlotId    → 预算请求过滤
//     res:discard:ssp:dspSlotId:dspSlotCode:sspSlotId    → 媒体响应过滤
//     res:discard:dsp:dspSlotId:dspSlotCode:sspSlotId    → 预算响应过滤
func processAllMetrics(ctx context.Context, redisKey string, metrics *DspRequestMetrics) error {
	// 标准化DspSlotCode
	//dspSlotId := normalizeDspSlotCode(string(metrics.DspSlotId))

	// 使用Redis Pipeline批量处理
	pipeline := global.EngineRedis.Pipeline()

	// ========== 1. 正常请求类 ==========

	// 媒体请求 (req:ssp:dspSlotId:dspSlotCode:sspSlotId)
	if metrics.SspReq > 0 {
		field := buildNormalField("req", "ssp", metrics.DspSlotId, metrics.DspSlotCode, metrics.SspSlotId)
		pipeline.HIncrBy(ctx, redisKey, field, int64(metrics.SspReq))
		logger.SystemLog.Debug().Msgf("媒体请求: key=%s, field=%s, count=%d", redisKey, field, metrics.SspReq)
	}

	// 预算请求 (req:dsp:dspSlotId:dspSlotCode:sspSlotId)
	if metrics.DspReq > 0 {
		field := buildNormalField("req", "dsp", metrics.DspSlotId, metrics.DspSlotCode, metrics.SspSlotId)
		pipeline.HIncrBy(ctx, redisKey, field, int64(metrics.DspReq))
		logger.SystemLog.Debug().Msgf("预算请求: key=%s, field=%s, count=%d", redisKey, field, metrics.DspReq)
	}

	// ========== 2. 正常响应类 ==========

	// 媒体响应 (res:ssp:dspSlotId:dspSlotCode:sspSlotId)
	if metrics.SspRes > 0 {
		field := buildNormalField("res", "ssp", metrics.DspSlotId, metrics.DspSlotCode, metrics.SspSlotId)
		pipeline.HIncrBy(ctx, redisKey, field, int64(metrics.SspRes))
		logger.SystemLog.Debug().Msgf("媒体响应: key=%s, field=%s, count=%d", redisKey, field, metrics.SspRes)
	}

	// 预算响应 (res:dsp:dspSlotId:dspSlotCode:sspSlotId)
	if metrics.DspRes > 0 {
		field := buildNormalField("res", "dsp", metrics.DspSlotId, metrics.DspSlotCode, metrics.SspSlotId)
		pipeline.HIncrBy(ctx, redisKey, field, int64(metrics.DspRes))
		logger.SystemLog.Debug().Msgf("预算响应: key=%s, field=%s, count=%d", redisKey, field, metrics.DspRes)
	}

	// ========== 3. 丢弃请求类 ==========

	// 媒体请求丢弃 (req:discard:ssp:dspSlotId:dspSlotCode:sspSlotId)
	if metrics.SspReqDiscard > 0 {
		field := buildDiscardField("req", "ssp", metrics.DspSlotId, metrics.DspSlotCode, metrics.SspSlotId)
		pipeline.HIncrBy(ctx, redisKey, field, int64(metrics.SspReqDiscard))
		logger.SystemLog.Debug().Msgf("媒体请求丢弃: key=%s, field=%s, count=%d", redisKey, field, metrics.SspReqDiscard)
	}

	// 预算请求丢弃 (req:discard:dsp:dspSlotId:dspSlotCode)
	if metrics.DspReqDiscard > 0 {
		field := buildDiscardField("req", "dsp", metrics.DspSlotId, metrics.DspSlotCode, metrics.SspSlotId)
		pipeline.HIncrBy(ctx, redisKey, field, int64(metrics.DspReqDiscard))
		logger.SystemLog.Debug().Msgf("预算请求丢弃: key=%s, field=%s, count=%d", redisKey, field, metrics.DspReqDiscard)
	}

	// ========== 4. 丢弃响应类 ==========

	// 媒体响应丢弃 (res:discard:ssp:dspSlotId:dspSlotCode)
	if metrics.SspResDiscard > 0 {
		field := buildDiscardField("res", "ssp", metrics.DspSlotId, metrics.DspSlotCode, metrics.SspSlotId)
		pipeline.HIncrBy(ctx, redisKey, field, int64(metrics.SspResDiscard))
		logger.SystemLog.Debug().Msgf("媒体响应丢弃: key=%s, field=%s, count=%d", redisKey, field, metrics.SspResDiscard)
	}

	// 预算响应丢弃 (res:discard:dsp:dspSlotId:dspSlotCode)
	if metrics.DspResDiscard > 0 {
		field := buildDiscardField("res", "dsp", metrics.DspSlotId, metrics.DspSlotCode, metrics.SspSlotId)
		pipeline.HIncrBy(ctx, redisKey, field, int64(metrics.DspResDiscard))
		logger.SystemLog.Debug().Msgf("预算响应丢弃: key=%s, field=%s, count=%d", redisKey, field, metrics.DspResDiscard)
	}

	// ========== 5. 设置过期时间 ==========
	pipeline.Expire(ctx, redisKey, 24*time.Hour)

	// ========== 6. 执行Pipeline ==========
	cmds, err := pipeline.Exec(ctx)
	if err != nil {
		// 记录哪个命令失败
		logger.SystemLog.Error().Msgf("Pipeline执行失败: %v, redisKey: %s, 命令数量: %d", err, redisKey, len(cmds))
		return err
	}
	logger.SystemLog.Debug().Msgf("Pipeline执行成功: redisKey=%s, 命令数量=%d", redisKey, len(cmds))
	return nil
}

// normalizeDspSlotCode 标准化DspSlotCode（处理空值）
//func normalizeDspSlotCode(dspSlotId int64) int64 {
//	dspSlotId = strings.TrimSpace(dspSlotId)
//	if dspSlotId == 0 {
//		return 0
//	}
//	return dspSlotId
//}

// buildNormalField 构建正常统计的Redis field
// 格式: {metricType}:{side}:{dspSlotId}:{dspSlotCode}:{sspSlotId}
// 例如: req:ssp:0:none, res:dsp:1:code123:sspSlotId
func buildNormalField(metricType, side string, dspSlotId int64, dspSlotCode string, sspSlotId int64) string {
	return fmt.Sprintf("%s:%s:%d:%s:%d", metricType, side, dspSlotId, dspSlotCode, sspSlotId)
}

// buildDiscardField 构建丢弃统计的Redis field
// 格式: {metricType}:discard:{side}:{dspSlotId}:{dspSlotCode}
// 例如: req:discard:ssp:0:none, res:discard:dsp:1:code123
func buildDiscardField(metricType, side string, dspSlotId int64, dspSlotCode string, sspSlotId int64) string {
	return fmt.Sprintf("%s:discard:%s:%d:%s:%d", metricType, side, dspSlotId, dspSlotCode, sspSlotId)
}

// alignToNext10Minutes 将时间戳对齐到下一个10分钟（向上取整）
// 例如: 202602252050 (20:50) → 202602252100 (21:00)
//       202602252005 (20:05) → 202602252010 (20:10)
func alignToNext10Minutes(eventTs string) string {
	return time.Now().Add(10 * time.Minute).Truncate(10 * time.Minute).Format("200601021504")
}

// buildRedisKey 构建Redis key
// 格式: {key}:{timeWindow}
// 例如: dsp:202602252100
func buildRedisKey(dsp, timeWindow string) string {
	return fmt.Sprintf("%s:%s", dsp, timeWindow)
}

// getCurrentEventTs 获取当前事件时间戳
func getCurrentEventTs() int64 {
	now := time.Now()
	eventTsStr := now.Format("200601021504")
	eventTs, _ := strconv.ParseInt(eventTsStr, 10, 64)
	return eventTs
}

// ==================== 调试和查询辅助函数 ====================

// TimeWindowMetricsDetail 时间窗指标详情（用于调试和查询）
type TimeWindowMetricsDetail struct {
	SspSlotId   int64
	TimeWindow  string
	DspSlotId   int64
	DspSlotCode string

	// 正常请求类
	SspReqCount int64 // 媒体请求
	DspReqCount int64 // 预算请求

	// 正常响应类
	SspResCount int64 // 媒体响应
	DspResCount int64 // 预算响应

	// 丢弃类（4种）
	SspReqDiscardCount int64 // 媒体请求过滤
	DspReqDiscardCount int64 // 预算请求过滤
	SspResDiscardCount int64 // 媒体响应过滤
	DspResDiscardCount int64 // 预算响应过滤
}

// GetTimeWindowMetricsDetail 获取指定时间窗的详细指标
func GetTimeWindowMetricsDetail(ctx context.Context, sspSlotId int64, timeWindow string) ([]TimeWindowMetricsDetail, error) {
	redisKey := buildRedisKey(global.EngineConfig.HashKey, timeWindow)

	// 获取整个Hash的所有field-value
	result, err := global.EngineRedis.HGetAll(ctx, redisKey).Result()
	if err != nil {
		return nil, err
	}

	// 按DspSlotId分组统计
	dspMetricsMap := make(map[string]*TimeWindowMetricsDetail)

	for field, countStr := range result {
		count, _ := strconv.ParseInt(countStr, 10, 64)
		if count == 0 {
			continue
		}

		// 解析field
		// 正常: {metricType}:{side}:{dspSlotId}:{dspSlotCode}
		// 丢弃: {metricType}:discard:{side}:{dspSlotId}:{dspSlotCode}
		parts := strings.Split(field, ":")

		var metricType, side string
		var dspSlotId int64
		var dspSlotCode string
		var isDiscard bool

		if len(parts) == 4 {
			// 正常格式: req:ssp:0:none
			metricType = parts[0] // req/res
			side = parts[1]       // ssp/dsp
			dspSlotId, _ = strconv.ParseInt(parts[2], 10, 64)
			dspSlotCode = parts[3]
			isDiscard = false
		} else if len(parts) == 5 {
			// 丢弃格式: req:discard:ssp:0:none
			metricType = parts[0] // req/res
			if parts[1] != "discard" {
				logger.SystemLog.Warn().Msgf("无效的field格式(期望discard): %s", field)
				continue
			}
			side = parts[2] // ssp/dsp
			dspSlotId, _ = strconv.ParseInt(parts[3], 10, 64)
			dspSlotCode = parts[4]
			isDiscard = true
		} else {
			logger.SystemLog.Warn().Msgf("无效的field格式: %s", field)
			continue
		}

		// 构建唯一key
		key := fmt.Sprintf("%d:%s", dspSlotId, dspSlotCode)

		if _, exists := dspMetricsMap[key]; !exists {
			dspMetricsMap[key] = &TimeWindowMetricsDetail{
				SspSlotId:   sspSlotId,
				TimeWindow:  timeWindow,
				DspSlotId:   dspSlotId,
				DspSlotCode: dspSlotCode,
			}
		}

		// 根据类型累加
		switch metricType {
		case "req":
			if !isDiscard {
				if side == "ssp" {
					dspMetricsMap[key].SspReqCount += count
				} else if side == "dsp" {
					dspMetricsMap[key].DspReqCount += count
				}
			} else {
				if side == "ssp" {
					dspMetricsMap[key].SspReqDiscardCount += count
				} else if side == "dsp" {
					dspMetricsMap[key].DspReqDiscardCount += count
				}
			}
		case "res":
			if !isDiscard {
				if side == "ssp" {
					dspMetricsMap[key].SspResCount += count
				} else if side == "dsp" {
					dspMetricsMap[key].DspResCount += count
				}
			} else {
				if side == "ssp" {
					dspMetricsMap[key].SspResDiscardCount += count
				} else if side == "dsp" {
					dspMetricsMap[key].DspResDiscardCount += count
				}
			}
		}
	}

	// 转换为数组
	resultList := make([]TimeWindowMetricsDetail, 0, len(dspMetricsMap))
	for _, metrics := range dspMetricsMap {
		resultList = append(resultList, *metrics)
	}

	return resultList, nil
}

// PrintTimeWindowMetrics 打印指定时间窗的所有指标（用于调试）
func PrintTimeWindowMetrics(ctx context.Context, sspSlotId int64, timeWindow string) {
	details, err := GetTimeWindowMetricsDetail(ctx, sspSlotId, timeWindow)
	if err != nil {
		logger.SystemLog.Error().Msgf("查询时间窗指标失败: %v, key: %s", err, buildRedisKey(global.EngineConfig.HashKey, timeWindow))
		return
	}

	logger.SystemLog.Info().Msgf("================== 时间窗指标: %s ==================", buildRedisKey(global.EngineConfig.HashKey, timeWindow))
	for _, detail := range details {
		logger.SystemLog.Info().Msgf("DspSlotId=%d, DspSlotCode=%s", detail.DspSlotId, detail.DspSlotCode)
		logger.SystemLog.Info().Msgf("  请求: 媒体=%d, 预算=%d", detail.SspReqCount, detail.DspReqCount)
		logger.SystemLog.Info().Msgf("  响应: 媒体=%d, 预算=%d", detail.SspResCount, detail.DspResCount)
		logger.SystemLog.Info().Msgf("  丢弃请求: 媒体=%d, 预算=%d", detail.SspReqDiscardCount, detail.DspReqDiscardCount)
		logger.SystemLog.Info().Msgf("  丢弃响应: 媒体=%d, 预算=%d", detail.SspResDiscardCount, detail.DspResDiscardCount)
		logger.SystemLog.Info().Msgf("  -----------------------------------")
	}
	logger.SystemLog.Info().Msgf("======================================================")
}

// DeleteTimeWindowMetrics 删除指定时间窗的指标（用于清理或测试）
func DeleteTimeWindowMetrics(ctx context.Context, sspSlotId int64, timeWindow string) error {
	redisKey := buildRedisKey(global.EngineConfig.HashKey, timeWindow)
	err := global.EngineRedis.Del(ctx, redisKey).Err()
	if err != nil {
		logger.SystemLog.Error().Msgf("删除时间窗指标失败: %v, key: %s", err, redisKey)
		return err
	}
	logger.SystemLog.Info().Msgf("删除时间窗指标成功: %s", redisKey)
	return nil
}

// GetAllKeysBySspSlotId 获取指定SSP的所有时间窗key（用于扫描）
func GetAllKeysBySspSlotId(ctx context.Context, sspSlotId int64) ([]string, error) {
	pattern := fmt.Sprintf("%d:*", sspSlotId)
	keys, err := global.EngineRedis.Keys(ctx, pattern).Result()
	if err != nil {
		return nil, err
	}
	return keys, nil
}
