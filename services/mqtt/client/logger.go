package client

import (
	"context"
	"github.com/goto/raccoon/logger"
	log "github.com/sirupsen/logrus"
)

type Logger struct {
	log *log.Logger
}

func (c Logger) Info(_ context.Context, msg string, attrs map[string]any) {
	c.log.Info(msg, attrs)
}
func (c Logger) Error(_ context.Context, err error, attrs map[string]any) {
	c.log.Error(err, attrs)
}

func NewLogger() Logger {
	return Logger{log: logger.GetLogger()}
}
