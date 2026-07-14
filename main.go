package main

import (
	"github.com/goto/raccoon/app"
	"github.com/goto/raccoon/config"
	"github.com/goto/raccoon/logger"
	"github.com/goto/raccoon/metrics"
)

func main() {
	config.Load()
	metrics.Setup()
	logger.SetLevel(config.Log.Level)
	config.LogConfig()
	err := app.Run()
	metrics.Close()
	if err != nil {
		logger.Fatal("init failure", err)
	}
}
