package config

type DbBase struct {
	Host         string `json:"host"yaml:"host"`
	Port         string `json:"port"yaml:"port"`
	Username     string `json:"username"yaml:"username"`
	Password     string `json:"password"yaml:"password"`
	DBname       string `json:"dbname"yaml:"dbname"`
	MaxIdleConns int    `json:"max_idle_conns"yaml:"max_idle_conns"`
	MaxOpenConns int    `json:"max_open_conns"yaml:"max_idle_conns"`
}
