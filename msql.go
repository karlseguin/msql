package main

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"os"
	"strings"

	"github.com/jpillora/opts"
	"github.com/karlseguin/msql/commands"
	"github.com/karlseguin/msql/driver"
	"github.com/karlseguin/msql/outputs"

	"github.com/knz/go-libedit"
	log "github.com/sirupsen/logrus"
)

type Command interface {
	Execute(context commands.Context, arguments string)
}

var (
	cmds = make(map[string]Command)
)

func init() {
	cmds["\\q"] = commands.Quit{}
	cmds["\\h"] = commands.Help{}
	cmds["\\?"] = commands.Help{}
	cmds["\\f"] = commands.Format{}
	cmds["\\x"] = commands.Expanded{}
}

func main() {
	type flags struct {
		Port     uint32 `opts:"help=port to connect to,short=p"`
		Host     string `opts:"help=host to connect to,short=h"`
		Database string `opts:"help=database to connect to,short=d"`
		UserName string `opts:"name=username,help=username to connect as,short=u"`
		Verbose  bool   `opts:"help=verbose logging, short=b`
		Quiet    bool   `opts:"help=quiet logging, short=q`
		Schema   string `opts:"help=schema to use when connecting, short=s`
		Role     string `opts:"help=role to use when connecting, short=r`
	}

	args := flags{
		Port:     50000,
		Host:     "127.0.0.1",
		Database: "monetdb",
		UserName: "monetdb",
		Verbose:  false,
		Quiet:    false,
		Schema:   "",
		Role:     "",
	}
	opts.Parse(&args)

	log.SetOutput(os.Stdout)
	if args.Verbose {
		log.SetLevel(log.InfoLevel)
	} else if args.Quiet {
		log.SetLevel(log.FatalLevel)
	} else {
		log.SetLevel(log.ErrorLevel)
	}

	preferences := loadPreferences()
	log.WithFields(log.Fields{"context": "preferences dump"}).Infof("historyFile = %s", preferences.historyFile)
	log.WithFields(log.Fields{"context": "preferences dump"}).Infof("passwordFile = %s", preferences.passwordFile)

	config := driver.Config{
		Host:     fmt.Sprintf("%s:%d", args.Host, args.Port),
		UserName: args.UserName,
		Database: args.Database,
		Password: getPassword(preferences, fmt.Sprintf("%s:%d:%s:%s:", args.Host, args.Port, args.Database, args.UserName)),
		Schema:   args.Schema,
		Role:     args.Role,
	}

	conn, err := driver.Open(config)
	if err != nil {
		log.WithFields(log.Fields{
			"host":    config.Host,
			"context": "connect to database",
		}).Fatal(err)
	}

	context := NewContext(conn, os.Stdout)
	defer context.Close()

	prompt, err := libedit.InitFiles("msql", true, os.Stdin, os.Stdout, os.Stderr)
	if err != nil {
		log.WithFields(log.Fields{"context": "libedit initialization"}).Fatal(err)
	}

	defer prompt.Close()
	prompt.RebindControlKeys()
	if err := prompt.UseHistory(500, true); err != nil {
		log.WithFields(log.Fields{"context": "libedit use history"}).Error(err)
	} else if preferences.historyFile != "" {
		prompt.LoadHistory(preferences.historyFile)
		prompt.SetAutoSaveHistory(preferences.historyFile, false)
	}

	for {
		prompt.SetLeftPrompt("todo] ")
		line, err := prompt.GetLine()
		if err != nil {
			if err == libedit.ErrInterrupted {
				return
			}
			log.WithFields(log.Fields{"context": "GetLine"}).Fatal(err)
		}

		if line == "\n" {
			// blank line, do nothing
			continue
		}

		if line[0] == '\\' {
			// any line that starts with \ is treated as a command
			command(context, strings.TrimRight(line, "\n"))
		} else {
			// any other line is treatment as the start of a statement
			statement(prompt, context, line)
		}
	}
}

// Commands are processed by this client itself. They're always single-lined.
func command(context *Context, line string) {
	if len(line) == 0 {
		return
	}

	args := ""
	cmd := line
	parts := strings.SplitN(line, " ", 2)

	if len(parts) == 2 {
		cmd = parts[0]
		args = parts[1]
	}

	c := cmds[cmd]
	if c == nil {
		log.Error("invalid command, type \\h for a list of commands")
		return
	}
	c.Execute(context, args)
}

// Statements are passed to the monetdb server for execution. Statements are
// semi-colon terminated and thus can span multiple lines. This function is
// essentially called when a non-command line is entered in the main loop. Once
// here, this function has its own readline loop to get the full statement.
func statement(prompt libedit.EditLine, context *Context, line string) {
	state := &state{}
	for {
		if state.add(line) {
			// we have a full statement, execute it
			statement := state.String()
			prompt.AddHistory(statement)
			prompt.SaveHistory()
			query(context, statement)
			return
		}
		prompt.SetLeftPrompt("")
		line, _ = prompt.GetLine()
	}
}

// The statement function has collected a full statement, send it to the server
// and deal with the response
func query(context *Context, statement string) {
	if err := context.conn.Send("s", statement); err != nil {
		handleDriverError(err) // can exit
		return
	}

	var err error
	if context.format == FORMAT_RAW {
		err = outputs.Raw(context.conn, context.out)
	} else if context.format == FORMAT_EXPANDED {
		err = outputs.Expanded(context.conn, context.out)
	} else {
		err = outputs.SQL(context.conn, context.out)
	}

	if err != nil {
		handleDriverError(err)
	}
}

// Tracks the state of our statement parsing
type state struct {
	// Accumlats the stament (one line at a time)
	bytes.Buffer

	// The literal delimiter, which will be either ' or " (or 0 if we're not
	// in a literal). This is the character we're looking for to end the literal
	literal byte

	// Whether the last character was an escape character or not. This tells us
	// to ignor the next character.
	escape bool
}

// We have a line from the user. We need to figure out whether there's a full
// statement in here or not. A full statement is delimited by a semi-colon, but
// that semi-colon can't be proceed by a \, and can't be inside a literal string
// (either single or double quoted)
func (s *state) add(line string) bool {
	escape := s.escape
	for i, c := range line {
		if escape {
			escape = false
			s.escape = false
			continue
		}
		if c == '\\' {
			escape = true
			s.escape = true
			continue
		}

		if c == ';' && s.literal == 0 {
			// We have a full statement statement. Add the line up to and including
			// (thus the +1) semi colon
			// TODO: figure out what to do with the rest
			s.WriteString(line[:i+1])
			return true
		}

		if c == '"' || c == '\'' {
			b := byte(c)
			if b == s.literal {
				s.literal = 0 // we're closing a matching pair
			} else if s.literal == 0 {
				s.literal = b //we're starting a new literal
			}
			// The else case is that this is a double quote inside a yet-unclosed
			// single quote, or a single quote inside a yet-unclosed doule quote.
			// Either way, it's just a value in a literal.
		}
	}

	// We don't have a full statement, add the line to our buffer as-is and get
	// more from the user
	s.WriteString(line)
	return false
}

func handleDriverError(err error) {
	if driverErr, ok := err.(driver.Error); ok && driverErr.Inner != nil {
		err = driverErr.Inner
	}
	if err == io.EOF {
		log.Fatal("connection closed")
	}

	if netErr, ok := err.(net.Error); ok && !netErr.Temporary() {
		log.Fatal(netErr)
	}
	log.Error(err)
}
