package day

import "time"

// 获取今天的时间 20060102
func NowTime() string {
	today := time.Now().Format("2006010215")
	return today
}

// 获取当前一小时的前一个小时的数据
func BeforeNowHour() string {
	hour := time.Now().Add(-1 * time.Hour).Format("2006010215")
	return hour
}
