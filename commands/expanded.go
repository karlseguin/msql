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
		context.Format("expanded")
		context.WriteString("Expanded display is on\n")
		return
	case "off":
		context.Format("sql")
		context.WriteString("Expanded display is off\n")
		return
	default:
		log.Error("valid options for \\x are: 'on' or 'off'")
	}
}
