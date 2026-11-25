package main

import (
	"os"
	_ "time/tzdata"

	_ "github.com/joho/godotenv/autoload"
	"github.com/target/goalert/app"
	_ "github.com/target/goalert/storagebackend/all"
	"github.com/target/goalert/util/log"
)

func main() {
	l := log.NewLogger()
	ctx := l.BackgroundContext()
	err := app.RootCmd.ExecuteContext(ctx)
	if err != nil {
		log.Log(ctx, err)
		os.Exit(1)
	}
}
