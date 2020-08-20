package commands

import (
	"fmt"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
)

type Describe struct {
}

func (cmd Describe) Execute(context Context, args string) {
	if args == "" {
		context.Query(`
			select s.name as Schema, t.name as Name, lower(tt.table_type_name) as Type
			from sys.tables t
			join sys.schemas s on t.schema_id = s.id
			join sys.table_types tt on t.type = tt.table_type_id
			where not t.system;
		`)
		return
	}

	table := args
	schema := context.Schema()
	parts := strings.SplitN(table, ".", 2)
	if len(parts) == 2 {
		schema = parts[0]
		table = parts[1]
	}

	if strings.HasSuffix(table, ";") {
		table = table[:len(table)-1]
	}

	conn := context.Conn()
	meta, err := conn.PrepareRow(`
		select t.name, t.query, t.type, t.id
		from sys.schemas s
			join sys.tables t on s.id = t.schema_id
		where s.name = ? and t.name = ?
	`, schema, table)

	if err != nil {
		log.WithFields(log.Fields{"context": "describe get meta", "schema": schema, "table": table}).Error(err)
		return
	}

	if meta == nil {
		context.WriteString(fmt.Sprintf("unknown %s\n", args))
		return
	}

	tableId, err := strconv.Atoi(meta[3])
	if err != nil {
		log.WithFields(log.Fields{"context": "describe table id", "schema": schema, "table": table}).Error(meta)
		return
	}

	columns, err := conn.PrepareRows(`
		select c.name, c.type, c.type_digits, c.type_scale, c."null", c."default"
		from sys._columns c
		where c.table_id = ?
		order by c.number
	`, tableId)

	if err != nil {
		log.WithFields(log.Fields{"context": "describe get columns", "schema": schema, "table": table}).Error(err)
		return
	}

	context.WriteString(fmt.Sprintf("create table %s.%s (\n", schema, table))

	for i, column := range columns {
		context.WriteString(fmt.Sprintf("  %s %s", column[0], column[1]))

		if column[4] == "false" {
			context.WriteString(" not null")
		} else {
			context.WriteString(" null")
		}
		if column[5] != "NULL" {
			context.WriteString(fmt.Sprintf(" default %s", column[4]))
		}
		if i == len(columns)-1 {
			context.WriteString("\n")
		} else {
			context.WriteString(",\n")
		}
	}
	context.WriteString(")\n")
}
