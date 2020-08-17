package main

import (
	"fmt"
	"io"
	"net/url"
	"strings"

	"github.com/karlseguin/msql/driver"
	log "github.com/sirupsen/logrus"
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
	user        string
	role        string
	schema      string
	host        string
	port        string
	database    string
	version     string
	release     string
	id          string
}

func NewContext(conn driver.Conn, out io.Writer) *Context {
	userRoleSchema, err := conn.QueryRow("select current_user, current_role, current_schema")
	if err != nil {
		log.WithFields(log.Fields{"context": "build context (1)"}).Error(err)
		userRoleSchema = []string{"unknown", "unknown", "unknown"}
	}

	version := "unknown"
	release := "unknown"
	urlString := "//unknown/unknown"
	envs, err := conn.QueryRows("select name, value from sys.env() where name in ('merovingian_uri', 'monet_release', 'monet_version')")
	if err != nil {
		log.WithFields(log.Fields{"context": "build context (2)"}).Error(err)
	}

	for _, env := range envs {
		switch env[0] {
		case "merovingian_uri":
			urlString = env[1]
			break
		case "monet_release":
			release = env[1]
			break
		case "monet_version":
			version = env[1]
			break
		}
	}

	if strings.HasPrefix(urlString, "mapi:") {
		urlString = urlString[5:]
	}
	parsed, err := url.Parse(urlString)
	if err != nil {
		log.WithFields(log.Fields{"context": "parse context url "}).Error(err)
		parsed, _ = url.Parse("//unknown/unknown")
	}

	parts := strings.Split(parsed.Host, ":")
	host := parts[0]
	port := "???"
	if len(parts) == 2 {
		port = parts[1]
	}
	database := strings.TrimLeft(parsed.Path, "/")

	return &Context{
		out:      out,
		conn:     conn,
		format:   FORMAT_SQL,
		user:     userRoleSchema[0],
		role:     userRoleSchema[1],
		schema:   userRoleSchema[2],
		host:     host,
		port:     port,
		database: database,
		version:  version,
		release:  release,
		id:       fmt.Sprintf("%s:%s/%s", host, port, database),
	}
}

func (c *Context) Close() {
	c.conn.Close()
}

func (c *Context) SetPrompt(prompt string) string {
	prompt = c.template(prompt)
	c.prompt = []byte(prompt)
	return prompt
}

func (c *Context) Prompt() {
	c.Write(c.prompt)
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

func (c *Context) template(t string) string {
	t = strings.ReplaceAll(t, "${user}", c.user)
	t = strings.ReplaceAll(t, "${role}", c.role)
	t = strings.ReplaceAll(t, "${schema}", c.schema)
	t = strings.ReplaceAll(t, "${host}", c.host)
	t = strings.ReplaceAll(t, "${port}", c.port)
	t = strings.ReplaceAll(t, "${database}", c.database)
	return t
}

func extractScalar(conn driver.Conn, query string, dflt string) string {
	log.WithFields(log.Fields{"context": "building context"}).Infof("Executing %s", query)
	if err := conn.Send(query); err != nil {
		log.WithFields(log.Fields{"context": "building context send"}).Error(err)
		return dflt
	}
	result, err := conn.ReadResult()
	if err != nil {
		log.WithFields(log.Fields{"context": "building context result"}).Error(err)
		return dflt
	}
	data, err := result.Next()
	if err != nil {
		log.WithFields(log.Fields{"context": "building context read"}).Error(err)
		return dflt
	}

	if len(data) == 0 {
		return dflt
	}
	return data[0][0]
}
