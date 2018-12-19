package utils

import (
	"github.com/jcelliott/lumber"
	"time"
)

func Return(start time.Time) {
	LumberPrefix(nil)
	lumber.Info("Elapsed time %s",time.Since(start))
}

