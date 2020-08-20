package commands

import "github.com/karlseguin/msql/driver"

type Context interface {
	WriteString(string)
	Format(string)
	Timing(bool)
	Query(string)
	Schema() string
	Conn() driver.Conn
}
