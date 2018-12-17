package api

import (
	"github.com/jcelliott/lumber"
	"github.com/spf13/cobra"
)

func LumberPrefix(cmd *cobra.Command) {

	prefix := "[sc]"
	if cmd != nil {
		prefix = cmd.Name()
	}
	lumber.Prefix("[" + prefix + "]")
}
