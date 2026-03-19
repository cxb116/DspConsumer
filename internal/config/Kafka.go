package config

import (
	"time"
)

type Kafka struct {
	Brokers       []string      `yaml:"brokers"`
	Frequency     time.Duration `yaml:"frequency"`
	Messages      int           `yaml:"messages"`
	ReturnSuccess bool          `yaml:"returnSuccess"`
	Topics        []string      `yaml:"topics"`
	GroupID       string        `yaml:"groupID"`
}
