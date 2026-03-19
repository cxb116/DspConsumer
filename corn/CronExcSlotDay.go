package corn

//TODO 集群下批量写入会重复的
// 小时表: 小时表每隔10分钟写入一次，每隔1小时创建一条数据,
// 天表:  每隔10分钟写入一次，每隔1天创建一条

//const (
//	DSP_HOUR_TABLE = "data_dsp_slot_hour_"
//	DSP_DAY_TABLE  = "data_dsp_slot_day_"
//	SSP_HOUR_TABLE = "data_ssp_slot_hour_"
//	SSP_DAY_TABLE  = "data_ssp_slot_day_"
//)
//
//// dsp,ssp 天表，每隔10分钟导入1次
//func CronExcSlotDay() error {
//
//	t := time.Now().Add(-10 * time.Minute).Truncate(10 * time.Minute)
//	key := t.Format("200601021504")
//
//	logger.SystemLog.Log().Msgf("indexKey:%s ", key)
//	ctx := context.Background()
//	data, err := global.EngineRedis.HGetAll(ctx, key).Result()
//	if err != nil {
//		logger.SystemLog.Log().Msgf("redis hgetall err:%s", err)
//		return err
//	}
//
//	for k, count := range data {
//		logger.SystemLog.Log().Msgf("数据:=====================================K %s, v %s", k, count)
//		if k != "" {
//			splits := FieldSplits(k)
//			if len(splits) > 4 {
//				logger.SystemLog.Info().Msgf("redis k 下的Field 异常:%s", splits)
//				return nil
//			}
//			DspDayTableName := DSP_DAY_TABLE + splits[2][:6]
//			SspDayTableName := SSP_DAY_TABLE + splits[2][:6]
//			logger.SystemLog.Log().Msgf("获取的表名: %s", DspDayTableName)
//
//			dspDayExists := DspTableExist(DspDayTableName, splits[3], splits[2][:8])
//			logger.SystemLog.Log().Msgf("查出来的数据条数 DspDayExists: %d", dspDayExists)
//
//			sspDayExists := SspTableExist(SspDayTableName, splits[3], splits[2][:8])
//			logger.SystemLog.Log().Msgf("查出来的数据条数 DspDayExists: %d", dspDayExists)
//
//			if splits[1] == "dsp" {
//				if dspDayExists == 0 { // 去插入当天的这条数据
//					sql := fmt.Sprintf(`INSERT INTO %s (dsp_slot_code, date) VALUES (?,?)`, DspDayTableName)
//					result := global.EngineDB.Exec(sql, splits[3], splits[2][:8])
//					if result.Error != nil {
//						logger.SystemLog.Log().Msgf("定时任务DspSlotHour insert err:%s", err)
//						return result.Error
//					}
//					DspUpdateData(splits[0], DspDayTableName, count, splits[3], splits[2][:8])
//				} else if dspDayExists == 1 { // 数据已经插入，现在修改
//					DspUpdateData(splits[0], DspDayTableName, count, splits[3], splits[2][:8])
//
//				} else {
//					panic(err)
//					logger.SystemLog.Log().Msgf("数据库数据异常,查出数据有多条:%s", k)
//				}
//			} else if splits[1] == "ssp" {
//				if sspDayExists == 0 { // 去插入当天的这条数据
//					sql := fmt.Sprintf(`INSERT INTO %s (ssp_slot_code, date) VALUES (?,?)`, SspDayTableName)
//					result := global.EngineDB.Exec(sql, splits[3], splits[2][:8])
//					if result.Error != nil {
//						logger.SystemLog.Log().Msgf("定时任务DspSlotHour insert err:%s", err)
//						return result.Error
//					}
//
//					SspUpdateData(splits[0], SspDayTableName, count, splits[3], splits[2][:8])
//
//				} else if sspDayExists == 1 { // 数据已经插入，现在修改
//					SspUpdateData(splits[0], SspDayTableName, count, splits[3], splits[2][:8])
//
//				} else {
//					panic(err)
//					logger.SystemLog.Log().Msgf("数据库数据异常,查出数据有多条:%s", k)
//				}
//			}
//		}
//
//	}
//	return nil
//
//}
//
//func SspUpdateData(splits0, tableName, count, splits3, spilts28 string) error {
//
//	if splits0 == "ims" {
//		sqls := fmt.Sprintf(`UPDATE %s SET show_pv = show_pv + ? where ssp_slot_code=? AND date=?`, tableName)
//		err := global.EngineDB.Exec(sqls, count, splits3, spilts28).Error
//		if err != nil {
//			panic(err)
//			return err
//		}
//	} else if splits0 == "cls" {
//		sql := fmt.Sprintf(`UPDATE %s SET click_pv = click_pv + ? where ssp_slot_code=? AND date=?`, tableName)
//		err := global.EngineDB.Exec(sql, count, splits3, spilts28).Error
//		if err != nil {
//			panic(err)
//			return err
//		}
//
//	} else if splits0 == "down" {
//		sql := fmt.Sprintf(`UPDATE %s SET complete_pv = complete_pv + ? where ssp_slot_code=? AND date=?`, tableName)
//		err := global.EngineDB.Exec(sql, count, splits3, spilts28).Error
//		if err != nil {
//			panic(err)
//			return err
//		}
//
//	} else if splits0 == "ins" {
//		sql := fmt.Sprintf(`UPDATE %s SET install_pv = install_pv + ? where ssp_slot_code=? AND date=?`, tableName)
//		err := global.EngineDB.Exec(sql, count, splits3, spilts28).Error
//		if err != nil {
//			panic(err)
//			return err
//		}
//
//	} else if splits0 == "act" {
//		sql := fmt.Sprintf(`UPDATE %s SET activate_pv = activate_pv + ? where ssp_slot_code=? AND date=?`, tableName)
//		err := global.EngineDB.Exec(sql, count, splits3, spilts28).Error
//		if err != nil {
//			panic(err)
//			return err
//		}
//
//	}
//	return nil
//}
//
//func DspUpdateData(splits0, tableName, count, splits3, spilts28 string) error {
//	if splits0 == "ims" {
//		sqls := fmt.Sprintf(`UPDATE %s SET show_pv = show_pv + ? where dsp_slot_code=? AND date=?`, tableName)
//		err := global.EngineDB.Exec(sqls, count, splits3, spilts28).Error
//		if err != nil {
//			panic(err)
//			return err
//		}
//	} else if splits0 == "cls" {
//		sql := fmt.Sprintf(`UPDATE %s SET click_pv = click_pv + ? where dsp_slot_code=? AND date=?`, tableName)
//		err := global.EngineDB.Exec(sql, count, splits3, spilts28).Error
//		if err != nil {
//			panic(err)
//			return err
//		}
//
//	} else if splits0 == "down" {
//		sql := fmt.Sprintf(`UPDATE %s SET complete_pv = complete_pv + ? where dsp_slot_code=? AND date=?`, tableName)
//		err := global.EngineDB.Exec(sql, count, splits3, spilts28).Error
//		if err != nil {
//			panic(err)
//			return err
//		}
//
//	} else if splits0 == "ins" {
//		sql := fmt.Sprintf(`UPDATE %s SET install_pv = install_pv + ? where dsp_slot_code=? AND date=?`, tableName)
//		err := global.EngineDB.Exec(sql, count, splits3, spilts28).Error
//		if err != nil {
//			panic(err)
//			return err
//		}
//
//	} else if splits0 == "act" {
//		sql := fmt.Sprintf(`UPDATE %s SET activate_pv = activate_pv + ? where dsp_slot_code=? AND date=?`, tableName)
//		err := global.EngineDB.Exec(sql, count, splits3, spilts28).Error
//		if err != nil {
//			panic(err)
//			return err
//		}
//
//	}
//	return nil
//}
//
//func DspTableExist(tableName, dspSlotCode, data string) int {
//	var count int
//
//	// 注意：表名不能用占位符，只能用 fmt.Sprintf 拼接
//	sql := fmt.Sprintf(`
//        SELECT COUNT(1)
//        FROM %s
//        WHERE dsp_slot_code = ? AND date = ?
//    `, tableName)
//
//	err := global.EngineDB.Debug().Raw(sql, dspSlotCode, data).Scan(&count).Error
//	if err != nil {
//		fmt.Println("查询表失败:", err)
//		return -1
//	}
//
//	return count
//}
//
//func SspTableExist(tableName, dspSlotCode, data string) int {
//	var count int
//
//	// 注意：表名不能用占位符，只能用 fmt.Sprintf 拼接
//	sql := fmt.Sprintf(`
//        SELECT COUNT(1)
//        FROM %s
//        WHERE ssp_slot_code = ? AND date = ?
//    `, tableName)
//
//	err := global.EngineDB.Debug().Raw(sql, dspSlotCode, data).Scan(&count).Error
//	if err != nil {
//		fmt.Println("查询表失败:", err)
//		return -1
//	}
//
//	return count
//}
//
//func FieldSplits(str string) []string {
//	parts := strings.Split(str, ":")
//	return parts
//}
