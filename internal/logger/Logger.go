package logger

import (
	"fmt"
	"os"
	"time"

	rotatelogs "github.com/lestrrat-go/file-rotatelogs"
	"github.com/rs/zerolog"
)

var (
	DataLog   zerolog.Logger
	ErrorLog  zerolog.Logger
	SystemLog zerolog.Logger
)

func GetExePaht() string {
	dir, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	return dir
}

func init() {

	// 创建目录
	_ = os.MkdirAll(GetExePaht()+"/log", 0755)

	// 控制台日志（系统日志）
	SystemLog = newConsoleLogger(os.Stdout)

	// 数据日志
	dataWriter, _ := newRotateWriter(GetExePaht() + "/log/data.log")
	DataLog = newFileLogger(dataWriter)

	// 错误日志
	errorWriter, _ := newRotateWriter(GetExePaht() + "/log/error.log")
	ErrorLog = newFileLogger(errorWriter)

}

//
// ----------- 日志格式模块 -----------
//

// 控制台 Logger（系统日志）
func newConsoleLogger(out *os.File) zerolog.Logger {
	cw := zerolog.ConsoleWriter{
		Out:        out,
		TimeFormat: "15:04:05", // HH:MM:SS
		NoColor:    false,
	}

	cw.FormatTimestamp = formatTime
	cw.FormatLevel = func(i interface{}) string { return "" }
	cw.FormatFieldName = func(i interface{}) string { return "" }
	cw.FormatFieldValue = func(i interface{}) string { return "" }
	cw.FormatMessage = func(i interface{}) string { return fmt.Sprintf("%v", i) }

	return zerolog.New(cw).With().Timestamp().Logger()
}

// 文件 Logger（数据日志 & 错误日志）
func newFileLogger(w *rotatelogs.RotateLogs) zerolog.Logger {
	cw := zerolog.ConsoleWriter{
		Out:        w,
		TimeFormat: "15:04:05",
		NoColor:    true,
	}

	cw.FormatTimestamp = formatTime
	cw.FormatLevel = func(i interface{}) string { return "" }
	cw.FormatFieldName = func(i interface{}) string { return "" }
	cw.FormatFieldValue = func(i interface{}) string { return "" }
	cw.FormatMessage = func(i interface{}) string { return fmt.Sprintf("%v", i) }

	return zerolog.New(cw).With().Timestamp().Logger()
}

// 关键：把 zerolog 的时间对象转换成 HH:MM:SS 字符串
func formatTime(i interface{}) string {
	switch t := i.(type) {
	case time.Time:
		return t.Format("15:04:05")
	case *time.Time:
		return t.Format("15:04:05")
	case string:
		if tt, err := time.Parse(time.RFC3339, t); err == nil {
			return tt.Format("15:04:05")
		}
	}
	return ""
}

//
// ----------- 日志切割模块 -----------
//

// 按小时切割日志，
func newRotateWriter(path string) (*rotatelogs.RotateLogs, error) {
	return rotatelogs.New(
		path+".%Y-%m-%d-%H",
		rotatelogs.WithMaxAge(7*24*time.Hour),
		rotatelogs.WithRotationTime(time.Hour),
		rotatelogs.WithLinkName(""), // 禁用 symlink，避免 windows 权限问题
	)
}
