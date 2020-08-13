package main

import (
	"io"

	"github.com/karlseguin/msql/driver"
)

const (
	FORMAT_RAW      = "raw"
	FORMAT_SQL      = "sql"
	FORMAT_EXPANDED = "expanded"
	FORMAT_TRASH    = "trash"
)

type Context struct {
	out         io.Writer
	err         io.Writer
	conn        driver.Conn
	preferences Preferences
	format      string
	timing      bool
	prompt      []byte
	exitOnError bool
}

func NewContext(conn driver.Conn, out io.Writer) *Context {
	return &Context{
		out:    out,
		conn:   conn,
		format: FORMAT_SQL,
	}
}

func (c *Context) Close() {
	c.conn.Close()
}

func (c *Context) Write(data []byte) {
	c.out.Write(data)
}

func (c *Context) WriteString(data string) {
	io.WriteString(c.out, data)
}

func (c *Context) Format(format string) {
	c.format = format
}
func (c *Context) Timing(on bool) {
	c.timing = on
}
