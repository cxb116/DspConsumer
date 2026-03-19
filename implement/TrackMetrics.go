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
	"github.com/go-redis/redis/v8"
)

// HandleImpressionMetrics 处理展现指标（ims）
func HandleImpressionMetrics(ctx context.Context, msgStr string) error {
	// 解析JSON消息
	var metrics ImsReportTalkMetrics
	if err := json.Unmarshal([]byte(msgStr), &metrics); err != nil {
		logger.SystemLog.Error().Msgf("解析展现指标消息失败: %v, msg: %s", err, msgStr)
		return err
	}

	// 数据校验
	if metrics.SspSlotId == 0 {
		logger.SystemLog.Warn().Msgf("SspSlotId为空，跳过处理")
		return fmt.Errorf("SspSlotId不能为空")
	}

	if metrics.EventTs == 0 {
		logger.SystemLog.Warn().Msgf("EventTs为空，使用当前时间")
		metrics.EventTs = getCurrentEventTs()
	}

	// 计算时间窗（向上对齐到10分钟）
	eventTsStr := strconv.FormatInt(metrics.EventTs, 10)
	timeWindowKey := alignToNext10Minutes(eventTsStr)

	// 构建Redis key
	redisKey := buildTrackRedisKey(global.EngineConfig.HashKey, timeWindowKey)

	// 构建展现field并统计
	field := buildTrackField("ims", metrics.DspSlotId, metrics.DspSlotCode, metrics.SspSlotId)
	if err := incrementTrackMetrics(ctx, redisKey, field); err != nil {
		logger.ErrorLog.Error().Msgf("展现统计失败: %v, key: %s, field: %s,sspSlotId: %d dspSlotId: %d", err, redisKey, field, metrics.SspSlotId, metrics.DspSlotId)
		return err
	}

	// 将成交价和利润也加入到redis 中
	//ReportPrice float64 `json:"reportPrice"` // RTB 的情况下媒体上报得到价格转换成int 类型 宏替换中解析的上报价格
	//Profit      float64 `json:"profit"`      // 利润  成交价 - 上报价格
	fieldReportPrice := buildTrackField("auction", metrics.DspSlotId, metrics.DspSlotCode, metrics.SspSlotId) // 成交价
	if err := auctionIncrementTrackMetrics(ctx, redisKey, fieldReportPrice, metrics.ReportPrice); err != nil {
		logger.SystemLog.Error().Msgf("成交价统计失败: %v, key: %s, field: %s", err, redisKey, fieldReportPrice)
		return err
	}
	fielProfit := buildTrackField("profit", metrics.DspSlotId, metrics.DspSlotCode, metrics.SspSlotId)
	if err := auctionIncrementTrackMetrics(ctx, redisKey, fielProfit, metrics.Profit); err != nil {
		logger.SystemLog.Error().Msgf("利润统计失败: %v, key: %s, field: %s", err, redisKey, fielProfit)
		return err
	}

	logger.SystemLog.Info().Msgf(
		"展现统计成功 - key: %s, field: %s, SspSlotId: %d, DspSlotId: %d, DspSlotCode: %s",
		redisKey, field, metrics.SspSlotId, metrics.DspSlotId, metrics.DspSlotCode,
	)
	return nil
}

// HandleClickMetrics 处理点击指标（clk）
func HandleClickMetrics(ctx context.Context, msgStr string) error {
	// 解析JSON消息
	var metrics ClkReportTalkMetrics
	if err := json.Unmarshal([]byte(msgStr), &metrics); err != nil {
		logger.SystemLog.Error().Msgf("解析点击指标消息失败: %v, msg: %s", err, msgStr)
		return err
	}

	// 数据校验
	if metrics.SspSlotId == 0 {
		logger.SystemLog.Warn().Msgf("SspSlotId为空，跳过处理")
		return fmt.Errorf("SspSlotId不能为空")
	}

	if metrics.EventTs == 0 {
		logger.SystemLog.Warn().Msgf("EventTs为空，使用当前时间")
		metrics.EventTs = getCurrentEventTs()
	}

	// 计算时间窗（向上对齐到10分钟）
	eventTsStr := strconv.FormatInt(metrics.EventTs, 10)
	timeWindowKey := alignToNext10Minutes(eventTsStr)

	// 构建Redis key
	redisKey := buildTrackRedisKey(global.EngineConfig.HashKey, timeWindowKey)

	// 构建点击field并统计
	field := buildTrackField("clk", metrics.DspSlotId, metrics.DspSlotCode, metrics.SspSlotId)
	if err := incrementTrackMetrics(ctx, redisKey, field); err != nil {
		logger.SystemLog.Error().Msgf("点击统计失败: %v, key: %s, field: %s", err, redisKey, field)
		return err
	}

	logger.SystemLog.Info().Msgf(
		"点击统计成功 - key: %s, field: %s, SspSlotId: %d, DspSlotId: %d, DspSlotCode: %s",
		redisKey, field, metrics.SspSlotId, metrics.DspSlotId, metrics.DspSlotCode,
	)

	return nil
}

// buildTrackRedisKey 构建展现/点击的Redis key
// 格式: {sspSlotId}:{timeWindow}
// 例如: 1:202602261320
func buildTrackRedisKey(key, timeWindow string) string {
	return fmt.Sprintf("%s:%s", key, timeWindow)
}

// buildTrackField 构建展现/点击的Redis field
// 格式: {trackType}:{dspSlotId}:{dspSlotCode}
// trackType: ims(展现) / clk(点击)
// 例如: ims:1:SenmengDspSlotCode, clk:2:TEST_SLOT_001
func buildTrackField(trackType string, dspSlotId int64, dspSlotCode string, sspSlotId int64) string {
	return fmt.Sprintf("%s:%d:%s:%d", trackType, dspSlotId, dspSlotCode, sspSlotId)
}

// normalizeTrackDspSlotCode 标准化DspSlotCode（处理空值）
//func normalizeTrackDspSlotCode(dspSlotCode string) string {
//	dspSlotCode = strings.TrimSpace(dspSlotCode)
//	if dspSlotCode == "" {
//		return "none"
//	}
//	return dspSlotCode
//}

// incrementTrackMetrics 在Redis中增加展现/点击计数
func incrementTrackMetrics(ctx context.Context, key, field string) error {
	// 使用管道提高性能
	pipeline := global.EngineRedis.Pipeline()

	// 增加计数
	pipeline.HIncrBy(ctx, key, field, 1)

	// 设置过期时间：24小时
	pipeline.Expire(ctx, key, 24*time.Hour)

	// 执行管道命令
	cmds, err := pipeline.Exec(ctx)
	if err != nil {
		logger.SystemLog.Error().Msgf("Pipeline执行失败: %v, redisKey: %s, 命令数量: %d", err, key, len(cmds))
		return err
	}
	logger.SystemLog.Debug().Msgf("Pipeline执行成功: redisKey=%s, 命令数量=%d", key, len(cmds))
	return nil
}

func auctionIncrementTrackMetrics(ctx context.Context, key, field string, price float64) error {
	// 使用管道提高性能
	pipeline := global.EngineRedis.Pipeline()

	// 增加计数
	pipeline.HIncrBy(ctx, key, field, int64(price))

	// 设置过期时间：24小时
	pipeline.Expire(ctx, key, 12*time.Hour)

	// 执行管道命令
	cmds, err := pipeline.Exec(ctx)
	if err != nil {
		logger.SystemLog.Error().Msgf("Pipeline执行失败: %v, redisKey: %s, 命令数量: %d", err, key, len(cmds))
		return err
	}
	logger.SystemLog.Debug().Msgf("Pipeline执行成功: redisKey=%s, 命令数量=%d", key, len(cmds))
	return nil
}

// ==================== 调试和查询辅助函数 ====================

// TrackMetricsDetail 展现/点击指标详情
type TrackMetricsDetail struct {
	SspSlotId   int64
	TimeWindow  string
	DspSlotId   int64
	DspSlotCode string
	ImsCount    int64   // 展现次数
	ClkCount    int64   // 点击次数
	CTR         float64 // 点击率 CTR = Clk / Ims
}

// GetTrackMetricsDetail 获取指定时间窗的展现/点击指标
func GetTrackMetricsDetail(ctx context.Context, sspSlotId int64, timeWindow string) ([]TrackMetricsDetail, error) {
	redisKey := buildTrackRedisKey(global.EngineConfig.HashKey, timeWindow)

	// 获取整个Hash的所有field-value
	result, err := global.EngineRedis.HGetAll(ctx, redisKey).Result()
	if err != nil && err != redis.Nil {
		return nil, err
	}

	// 如果key不存在或为空，返回空数组
	if len(result) == 0 {
		return []TrackMetricsDetail{}, nil
	}

	// 按DspSlotId分组统计
	trackMetricsMap := make(map[string]*TrackMetricsDetail)

	for field, countStr := range result {
		count, _ := strconv.ParseInt(countStr, 10, 64)
		if count == 0 {
			continue
		}

		// 解析field: {trackType}:{dspSlotId}:{dspSlotCode}clk
		// 例如: ims:1:SenmengDspSlotCode, clk:2:TEST_SLOT_001
		parts := strings.Split(field, ":")
		if len(parts) != 4 {
			logger.SystemLog.Warn().Msgf("无效的field格式: %s", field)
			continue
		}

		trackType := parts[0] // ims/clk
		dspSlotId, _ := strconv.ParseInt(parts[1], 10, 64)
		dspSlotCode := parts[2]

		// 构建唯一key
		key := fmt.Sprintf("%d:%s", dspSlotId, dspSlotCode)

		if _, exists := trackMetricsMap[key]; !exists {
			trackMetricsMap[key] = &TrackMetricsDetail{
				SspSlotId:   sspSlotId,
				TimeWindow:  timeWindow,
				DspSlotId:   dspSlotId,
				DspSlotCode: dspSlotCode,
			}
		}

		// 根据类型累加
		switch trackType {
		case "ims":
			trackMetricsMap[key].ImsCount += count
		case "clk":
			trackMetricsMap[key].ClkCount += count
		}

		// 计算CTR
		if trackMetricsMap[key].ImsCount > 0 {
			trackMetricsMap[key].CTR = float64(trackMetricsMap[key].ClkCount) / float64(trackMetricsMap[key].ImsCount) * 100
		}
	}

	// 转换为数组
	resultList := make([]TrackMetricsDetail, 0, len(trackMetricsMap))
	for _, metrics := range trackMetricsMap {
		resultList = append(resultList, *metrics)
	}

	return resultList, nil
}

//// PrintTrackMetrics 打印指定时间窗的展现/点击指标（用于调试）
//func PrintTrackMetrics(ctx context.Context, sspSlotId int64, timeWindow string) {
//	details, err := GetTrackMetricsDetail(ctx, sspSlotId, timeWindow)
//	if err != nil {
//		logger.SystemLog.Error().Msgf("查询展现/点击指标失败: %v, key: %s", err, buildTrackRedisKey(global.EngineConfig.HashKey, timeWindow))
//		return
//	}
//
//	if len(details) == 0 {
//		logger.SystemLog.Info().Msgf("时间窗 %s 无数据", buildTrackRedisKey(global.EngineConfig.HashKey, timeWindow))
//		return
//	}
//
//	logger.SystemLog.Info().Msgf("================== 展现/点击指标: %s ==================", buildTrackRedisKey(global.EngineConfig.HashKey, timeWindow))
//	for _, detail := range details {
//		logger.SystemLog.Info().Msgf("DspSlotId=%d, DspSlotCode=%s", detail.DspSlotId, detail.DspSlotCode)
//		logger.SystemLog.Info().Msgf("  展现次数: %d", detail.ImsCount)
//		logger.SystemLog.Info().Msgf("  点击次数: %d", detail.ClkCount)
//		logger.SystemLog.Info().Msgf("  点击率(CTR): %.2f%%", detail.CTR)
//		logger.SystemLog.Info().Msgf("  -----------------------------------")
//	}
//	logger.SystemLog.Info().Msgf("======================================================")
//}
//
//// DeleteTrackMetrics 删除指定时间窗的展现/点击指标（用于清理或测试）
//func DeleteTrackMetrics(ctx context.Context, sspSlotId int64, timeWindow string) error {
//	redisKey := buildTrackRedisKey(global.EngineConfig.HashKey, timeWindow)
//	err := global.EngineRedis.Del(ctx, redisKey).Err()
//	if err != nil {
//		logger.SystemLog.Error().Msgf("删除展现/点击指标失败: %v, key: %s", err, redisKey)
//		return err
//	}
//	logger.SystemLog.Info().Msgf("删除展现/点击指标成功: %s", redisKey)
//	return nil
//}
//
//// GetAllTrackKeysBySspSlotId 获取指定SSP的所有时间窗key（用于扫描）
//func GetAllTrackKeysBySspSlotId(ctx context.Context, sspSlotId int64) ([]string, error) {
//	pattern := fmt.Sprintf("%d:*", sspSlotId)
//	keys, err := global.EngineRedis.Keys(ctx, pattern).Result()
//	if err != nil {
//		return nil, err
//	}
//	return keys, nil
//}
