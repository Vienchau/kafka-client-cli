package cmd

import "github.com/spf13/cobra"

var versionCmd = &cobra.Command{
	Use:     "version",
	Aliases: []string{"v"},
	Short:   "Print the version number of kcli",
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Println("kcli version: ", version)
		cmd.Println("Build time: ", buildTime)
	},
}
