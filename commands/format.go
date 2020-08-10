package commands

import (
	"strings"

	log "github.com/sirupsen/logrus"
)

type Format struct {
}

func (cmd Format) Execute(context Context, args string) {
	switch strings.ToLower(args) {
	case "raw":
		context.FormatRaw()
		return
	case "sql":
		context.FormatSQL()
		return
	case "expanded":
		context.FormatExpanded()
		return
	default:
		log.Error("valid formats for \f are: 'raw', 'sql' and 'expanded'")
	}
}
