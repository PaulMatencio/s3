package utils

import (
	"github.com/jcelliott/lumber"
	"time"
)

func Return(start time.Time) {
	lumber.Info("Elapsed time %s",time.Since(start))
	LumberPrefix(nil)
}

