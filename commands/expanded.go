package commands

import (
	"strings"

	log "github.com/sirupsen/logrus"
)

type Expanded struct {
}

func (cmd Expanded) Execute(context Context, args string) {
	switch strings.ToLower(args) {
	case "on":
		context.FormatExpanded()
		return
	case "sql":
		context.FormatSQL()
		return
	default:
		log.Error("valid options for \\x are: 'on' or 'off'")
	}
}
