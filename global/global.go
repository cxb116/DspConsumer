package global

import (
	"github.com/IBM/sarama"
	"github.com/cxb116/consumerManagers/internal/config"
	"github.com/go-redis/redis/v8"
	"github.com/spf13/viper"
	"gorm.io/gorm"
)

var (
	EngineViper  *viper.Viper
	EngineConfig config.Server // 引擎配置
	EngineDB     *gorm.DB

	EngineRedis *redis.Client
	EngineKafka *sarama.KafkaVersion
)

const (
	SSP = 1
	DSP = 0
)

const (
	IMS  = 0
	CLS  = 1
	DOWN = 2
	INS  = 3
	ACT  = 4
)
