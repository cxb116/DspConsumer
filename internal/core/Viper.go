package core

import (
	"flag"
	"fmt"
	"github.com/cxb116/consumerManagers/global"

	"github.com/spf13/viper"
)

func Viper() *viper.Viper {

	config := getConfigPath()

	v := viper.New()
	v.SetConfigFile(config)
	v.SetConfigType("yaml")
	err := v.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("Fatal error config file: %s \n", err))
	}

	//v.WatchConfig()
	//v.OnConfigChange(func(e fsnotify.Event) {
	//	fmt.Println("Config file changed:", e.Name)
	//	if err := v.Unmarshal(&global.EngineConfig); err != nil {
	//		fmt.Println(err)
	//	}
	//})

	if err := v.Unmarshal(&global.EngineConfig); err != nil {
		fmt.Println(err)
	}
	fmt.Println("Viper load success", global.EngineConfig.Kafka)
	return v
}

func getConfigPath() (config string) {
	flag.StringVar(&config, "c", "./config.yaml", "choose config file.")
	flag.Parse() // 解析命令行参数

	if config != "" { // 命令行参数不为空 将值赋值于config
		fmt.Printf("您正在使用命令行的 '-c' 参数传递的值, config 的路径为 %s\n", config)
		return
	}

	return "./config/config.yaml"
}
