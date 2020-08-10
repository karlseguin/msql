package main

import (
	"io"

	"github.com/karlseguin/msql/driver"
)

const (
	FORMAT_RAW      = 0
	FORMAT_SQL      = 1
	FORMAT_EXPANDED = 2
)

type Context struct {
	out         io.Writer
	err         io.Writer
	conn        driver.Conn
	preferences Preferences
	format      uint8
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

func (c *Context) Output(data []byte) {
	c.out.Write(data)
}

func (c *Context) FormatRaw() {
	c.format = FORMAT_RAW
}
func (c *Context) FormatSQL() {
	c.format = FORMAT_SQL
}
func (c *Context) FormatExpanded() {
	c.format = FORMAT_EXPANDED
}
