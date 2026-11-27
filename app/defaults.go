package app

import "time"

// Defaults returns the default app config.
func Defaults() Config {
	return Config{
		ListenAddr:        "localhost:8081",
		MaxReqBodyBytes:   256 * 1024,
		MaxReqHeaderBytes: 4096,
		EngineCycleTime:   5 * time.Second,
	}
}
