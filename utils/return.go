package utils

import (
	"github.com/paulmatencio/s3/gLog"
	"time"
)

func Return(start time.Time) {
	gLog.Info.Printf("Elapsed time %s",time.Since(start))
	// LumberPrefix(nil)
}

