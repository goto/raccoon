package app

import (
	"context"

	"github.com/goto/raccoon/logger"
)

// Run the server
func Run() error {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	//@TODO - init config
	shutdown := make(chan bool)
	//start server
	StartServer(ctx, cancel, shutdown)
	logger.Info("App.Run --> Complete")
	<-shutdown
	return nil
}
