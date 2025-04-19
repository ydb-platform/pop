package cmd

import (
	"github.com/spf13/cobra"
	"github.com/ydb-platform/pop/v6/soda/cmd/generate"
)

var generateCmd = &cobra.Command{
	Use:     "generate",
	Aliases: []string{"g"},
	Short:   "Generates config, model, and migrations files.",
}

func init() {
	generateCmd.AddCommand(generate.ConfigCmd)
	generateCmd.AddCommand(generate.FizzCmd)
	generateCmd.AddCommand(generate.SQLCmd)
	generateCmd.AddCommand(generate.ModelCmd)
	RootCmd.AddCommand(generateCmd)
}
