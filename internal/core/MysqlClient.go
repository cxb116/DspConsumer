package core

import (
	"fmt"
	"github.com/cxb116/consumerManagers/global"
	"github.com/cxb116/consumerManagers/internal/config"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"log"
)

func NewClientMysql() *gorm.DB {
	if global.EngineConfig.Database.Username != "" {
		database := initMysqlDatabase(global.EngineConfig.Database)
		return database
	}
	return nil
}

func initMysqlDatabase(p config.DbBase) *gorm.DB {
	if p.Username == "" {
		return nil
	}

	dsn := fmt.Sprintf(
		"%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		p.Username, p.Password, p.Host, p.Port, p.DBname,
	)

	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Println("mysql connection fail")
		panic(err)
		return nil
	}

	sqlDB, _ := db.DB()
	sqlDB.SetMaxIdleConns(p.MaxIdleConns)
	sqlDB.SetMaxOpenConns(p.MaxOpenConns)
	log.Println("mysql connection success!")
	return db
}
