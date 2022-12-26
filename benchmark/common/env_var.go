package common

import (
	"fmt"
	"os"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func SetLogLevelFromEnv() {
	logLevel := os.Getenv("LOG_LEVEL")
	if level, err := zerolog.ParseLevel(logLevel); err == nil {
		zerolog.SetGlobalLevel(level)
	} else {
		zerolog.SetGlobalLevel(zerolog.WarnLevel)
	}

	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
}

func CheckCacheConfig() bool {
	useCacheStr := os.Getenv("USE_CACHE")
	useCache := useCacheStr == "true" || useCacheStr == "1"
	fmt.Fprintf(os.Stderr, "use cacheStr: %s, use cache: %v\n", useCacheStr, useCache)
	return useCache
}
