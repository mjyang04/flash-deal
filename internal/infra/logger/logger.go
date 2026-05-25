// Package logger provides a process-wide zap logger configured for prod or dev.
package logger

import (
	"sync"

	"go.uber.org/zap"
)

var (
	once sync.Once
	log  *zap.Logger
)

// Init configures the singleton. mode = "dev" enables console+colors,
// any other value uses production JSON encoding.
func Init(mode string) (*zap.Logger, error) {
	var err error
	once.Do(func() {
		if mode == "dev" {
			log, err = zap.NewDevelopment()
		} else {
			log, err = zap.NewProduction()
		}
	})
	return log, err
}

// L returns the initialized logger. Call Init first; otherwise this returns a no-op.
func L() *zap.Logger {
	if log == nil {
		return zap.NewNop()
	}
	return log
}
