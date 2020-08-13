package commands

import (
	"strings"

	log "github.com/sirupsen/logrus"
)

type Timing struct {
}

func (cmd Timing) Execute(context Context, args string) {
	switch strings.ToLower(args) {
	case "on":
		context.Timing(true)
		context.WriteString("Timing is on\n")
		return
	case "off":
		context.Timing(false)
		context.WriteString("Timing is off\n")
		return
	default:
		log.Error("valid options for \\timing are: 'on' or 'off'")
	}
}
