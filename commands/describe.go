package commands

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/karlseguin/msql/driver"
	log "github.com/sirupsen/logrus"
)

type Describe struct {
}

func (describe Describe) Execute(context Context, args string) {
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
		log.WithFields(log.Fields{"context": "describe: meta", "schema": schema, "table": table}).Error(err)
		return
	}

	if meta == nil {
		context.WriteString(fmt.Sprintf("unknown %s\n", args))
		return
	}

	tableId, err := strconv.Atoi(meta[3])
	if err != nil {
		log.WithFields(log.Fields{"context": "describe: id", "schema": schema, "table": table}).Error(meta)
		return
	}

	tpe := meta[2]
	if tpe == "0" {
		describe.table(context, tableId, conn, table)
	} else if tpe == "1" {
		context.WriteString(meta[1])
		context.WriteString("\n\n")
	} else {
		log.Errorf("don't know how to describe type: %s", tpe)
	}
	// todo: support views and other stuff, I think based on meta[2] (the type)
}

func (describe Describe) table(context Context, tableId int, conn driver.Conn, table string) {
	if !describe.createTable(context, tableId, conn, table) {
		return
	}
	if !describe.primaryKey(context, tableId, conn, table) {
		return
	}

	// close the create table X (...
	context.WriteString("\n);\n")
	if !describe.foreignKeys(context, tableId, conn, table) {
		return
	}
	context.WriteString("\n")
}

func (describe Describe) createTable(context Context, tableId int, conn driver.Conn, table string) bool {
	columns, err := conn.PrepareRows(`
		select c.name, c.type, c.type_digits, c.type_scale, c."null", c."default"
		from sys._columns c
		where c.table_id = ?
		order by c.number
	`, tableId)

	if err != nil {
		log.WithFields(log.Fields{"context": "describe table: columns", "tableId": tableId, "table": table}).Error(err)
		return false
	}

	context.WriteString(fmt.Sprintf("create table %s(\n", table))

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
		if i < len(columns)-1 {
			context.WriteString(",\n")
		}
	}
	return true
}

func (describe Describe) primaryKey(context Context, tableId int, conn driver.Conn, table string) bool {
	pks, err := conn.PrepareRows(`
		select col.name, key.name
		from sys.objects col
			join sys.keys key using (id)
		where key.type = 0 and key.table_id = ?
	`, tableId)

	if err != nil {
		log.WithFields(log.Fields{"context": "describe table: pk", "tableId": tableId, "table": table}).Error(err)
		return false
	}

	if len(pks) == 0 {
		return true
	}

	name := pks[0][1]
	columns := make([]string, len(pks))
	for i, row := range pks {
		columns[i] = row[0]
	}
	// ,\n because we didn't know if the last column needed it or not (there could
	// be no pk, in which case, there should be no comma0. Now we know there is a
	// PK
	context.WriteString(fmt.Sprintf(",\n  primary key (%s) -- %s", strings.Join(columns, ", "), name))
	return true
}

func (describe Describe) foreignKeys(context Context, tableId int, conn driver.Conn, table string) bool {
	fks, err := conn.PrepareRows(`
		select ref_t.name, ref_cols.name, table_cols.name, ref_keys.name, ref_keys."action"
		from sys._tables table,
			sys.objects table_cols,
			sys.keys ref_keys,
			sys._tables ref_t,
			sys.objects ref_cols,
			sys.keys table_keys,
			sys.schemas ps
		where table.id = ref_keys.table_id
		 and ref_t.id = table_keys.table_id
		 and ref_keys.id = table_cols.id
		 and table_keys.id = ref_cols.id
		 and ref_keys.rkey = table_keys.id
		 and table_cols.nr = ref_cols.nr
		 and ref_t.schema_id = ps.id
		 and table.id = ?
	`, tableId)

	if err != nil {
		log.WithFields(log.Fields{"context": "describe table: fk", "tableId": tableId, "table": table}).Error(err)
		return false
	}

	if len(fks) == 0 {
		return true
	}

	context.WriteString("\nForeign Keys:\n")
	// todo use action for on delete / on update
	format := "  %s foreign key (%s) references %s(%s);\n"
	for _, fk := range fks {
		context.WriteString(fmt.Sprintf(format, fk[3], fk[2], fk[0], fk[1]))
	}
	return true
}
