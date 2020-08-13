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
		context.Format("raw")
		context.WriteString("Raw display is on\n")
		return
	case "sql":
		context.Format("sql")
		context.WriteString("SQL display is on\n")
		return
	case "expanded":
		context.Format("expanded")
		context.WriteString("Expanded display is on\n")
		return
	case "trash":
		context.Format("trash")
		context.WriteString("Trash display is on\n")
		return
	default:
		log.Error("valid formats for \\f are: 'raw', 'sql' and 'expanded'")
	}
}
