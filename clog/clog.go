package clog

import (
	"go.uber.org/zap"
	"github.com/natefinch/lumberjack"
	"go.uber.org/zap/zapcore"
	"sync"
)

var once sync.Once
var logger *zap.SugaredLogger

func InitLogger(logpath string, loglevel string) {
	once.Do(func() {
		logger = initLogger(logpath, loglevel)
	})
}

// logpath 日志文件路径
// loglevel 日志级别
func initLogger(logpath string, loglevel string) *zap.SugaredLogger {

    hook := lumberjack.Logger{
        Filename:   logpath, // 日志文件路径
        MaxSize:    128,     // megabytes
        MaxBackups: 30,      // 最多保留300个备份
        MaxAge:     7,       // days
        Compress:   true,    // 是否压缩 disabled by default
    }

    w := zapcore.AddSync(&hook)

    // 设置日志级别,debug可以打印出info,debug,warn；info级别可以打印warn，info；warn只能打印warn
    // debug->info->warn->error
    var level zapcore.Level
    switch loglevel {
    case "debug":
        level = zap.DebugLevel
    case "info":
        level = zap.InfoLevel
    case "error":
        level = zap.ErrorLevel
    default:
        level = zap.InfoLevel
    }
    encoderConfig := zap.NewProductionEncoderConfig()
    // 时间格式
    encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
    core := zapcore.NewCore(
        zapcore.NewConsoleEncoder(encoderConfig),
        w,
        level,
    )

    logger := zap.New(core).Sugar()
    logger.Info("DefaultLogger init success")

    return logger
}

func Info(args ...interface{}) {
	logger.Info(args...)
}

func Debug(args ...interface{}) {
	logger.Debug(args...)
}

func Warn(args ...interface{}) {
	logger.Warn(args...)
}

func Error(args ...interface{}) {
	logger.Error(args...)
}

func Infof(format string, args ...interface{}) {
	logger.Infof(format, args...)
}

func Debugf(format string, args ...interface{}) {
	logger.Debugf(format, args...)
}

func Warnf(format string, args ...interface{}) {
	logger.Warnf(format, args...)
}

func Errorf(format string, args ...interface{}) {
	logger.Errorf(format, args...)
}