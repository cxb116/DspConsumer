package config

import "time"

type Server struct {
	Version        string        `json:"version" yaml:"version"`
	Port           string        `json:"port" yaml:"port"`
	ReadTimeout    time.Duration `json:"read_timeout" yaml:"read_timeout"`
	WriteTimeout   time.Duration `json:"write_timeout" yaml:"write_timeout"`
	IdleTimeout    time.Duration `json:"idle_timeout" yaml:"idle_timeout"`
	MaxHeaderBytes int           `json:"max_header_bytes" yaml:"max_header_bytes"`
	WorkerSize     int           `json:"workerSize" yaml:"workerSize"`
	TaskQueue      int           `json:"taskQueue" yaml:"taskQueue"`
	HashKey        string        `json:"hash_key" yaml:"hash_key"`
	Kafka          Kafka         `json:"kafka" yaml:"kafka"`
	Redis          Redis         `json:"redis" yaml:"redis"`
	Database       DbBase        `json:"database" yaml:"database"`
}
