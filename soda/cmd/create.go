package cmd

import (
	"github.com/spf13/cobra"
	"github.com/ydb-platform/pop/v6"
)

var createCmd = &cobra.Command{
	Use:   "create",
	Short: "Creates databases for you",
	RunE: func(cmd *cobra.Command, args []string) error {
		var err error
		if all {
			for _, conn := range pop.Connections {
				err = pop.CreateDB(conn)
				if err != nil {
					return err
				}
			}
		} else {
			err = pop.CreateDB(getConn())
		}
		return err
	},
}

func init() {
	createCmd.Flags().BoolVarP(&all, "all", "a", false, "Creates all of the databases in the database.yml")
	RootCmd.AddCommand(createCmd)
}
