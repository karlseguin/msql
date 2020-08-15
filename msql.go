package main

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"strings"
	"time"

	"github.com/jessevdk/go-flags"
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
	cmds["\\timing"] = commands.Timing{}
}

func main() {
	var opts struct {
		Port        uint32       `description:"port to connect to" short:"p" long:"port" default:"50000"`
		Host        string       `description:"host to connect to" short:"h" long:"host" default:"127.0.0.1"`
		Database    string       `description:"database to connect to" short:"d" long:"database" default:"monetdb"`
		UserName    string       `description:"username to connect as" short:"u" long:"username" default:"monetdb"`
		Verbose     bool         `description:"verbose logging" long:"verbose"`
		Quiet       bool         `description:"quiet logging" long:"quiet"`
		Schema      string       `description:"schema to use when connecting" short:"s" long:"schema"`
		Role        string       `description:"role to use when connecting" short:"r" long:"role"`
		Command     string       `description:"executes the command and exists" short:"c"`
		Format      string       `description:"default output format (sql|raw|expanded)" default:"sql"`
		ExitOnError bool         `description:"exit on error" long:"exit-on-error"`
		Help        func() error `description:"show this help screen" long:"help"`
		File        string       `description:"file to exist" long:"file" short:"f"`
	}

	parser := flags.NewParser(&opts, flags.Default & ^flags.HelpFlag)
	opts.Help = func() error {
		parser.WriteHelp(os.Stdout)
		os.Exit(1)
		return nil
	}
	_, err := parser.Parse()
	if err != nil {
		log.Fatal(err)
	}

	log.SetOutput(os.Stdout)
	if opts.Verbose {
		log.SetLevel(log.InfoLevel)
	} else if opts.Quiet {
		log.SetLevel(log.FatalLevel)
	} else {
		log.SetLevel(log.ErrorLevel)
	}

	preferences := loadPreferences()

	config := driver.Config{
		Host:     fmt.Sprintf("%s:%d", opts.Host, opts.Port),
		UserName: opts.UserName,
		Database: opts.Database,
		Password: getPassword(preferences, fmt.Sprintf("%s:%d:%s:%s:", opts.Host, opts.Port, opts.Database, opts.UserName)),
		Schema:   opts.Schema,
		Role:     opts.Role,
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

	context.Timing(preferences.timing)
	context.Format(strings.ToLower(opts.Format))
	context.exitOnError = opts.ExitOnError

	// handles -c or -f argument or stdin input
	conditionallyExecuteAndExit(opts.Command, opts.File, context)

	promptText := context.SetPrompt(preferences.prompt)
	prompt, err := libedit.InitFiles("msql", true, os.Stdin, os.Stdout, os.Stderr)
	if err != nil {
		log.WithFields(log.Fields{"context": "libedit initialization"}).Fatal(err)
	}

	defer prompt.Close()
	prompt.RebindControlKeys()
	if err := prompt.UseHistory(500, true); err != nil {
		log.WithFields(log.Fields{"context": "libedit use history"}).Error(err)
	} else if preferences.historyFile != "" {
		historyFile := context.template(preferences.historyFile)
		prompt.LoadHistory(historyFile)
		prompt.SetAutoSaveHistory(historyFile, false)
	}

	for {
		// we have to write the prompt ourselves incase it contains color codes (not
		// sure why they don't work through libedit)
		prompt.SetLeftPrompt(promptText)
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

	if c := cmds[cmd]; c != nil {
		c.Execute(context, args)
	} else if cmd == "\\d" || cmd == "\\d+" {
		query(context, `
			select s.name as Schema, t.name as Name, lower(tt.table_type_name) as Type
			from sys.tables t
			join sys.schemas s on t.schema_id = s.id
			join sys.table_types tt on t.type = tt.table_type_id
			where not t.system;
		`)
	} else if cmd == "\\du" {
		query(context, "select * from sys.users;")
	} else {
		log.Error("invalid command, type \\h for a list of commands")
	}
}

// Statements are passed to the monetdb server for execution. Statements are
// semi-colon terminated and thus can span multiple lines. This function is
// essentially called when a non-command line is entered in the main loop. Once
// here, this function has its own readline loop to get the full statement.
func statement(prompt libedit.EditLine, context *Context, line string) {
	state := &state{context: context}
	for {
		complete, rest := state.add(line)
		if complete {
			// we have a full statement, execute it
			sql := state.String()
			prompt.AddHistory(sql)
			prompt.SaveHistory()
			query(context, sql)
			if rest != "" {
				// not great, but it works
				context.Prompt()
				context.Write([]byte(rest)[1:])
				statement(prompt, context, rest)
				return
			}
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
		if context.exitOnError {
			os.Exit(1)
		}
		return
	}

	var err error
	var meta *driver.Meta
	start := time.Now()
	if context.format == FORMAT_RAW {
		meta, err = outputs.Raw(context.conn, context.out)
	} else if context.format == FORMAT_EXPANDED {
		meta, err = outputs.Expanded(context.conn, context.out)
	} else if context.format == FORMAT_TRASH {
		meta, err = outputs.Trash(context.conn)
	} else {
		meta, err = outputs.SQL(context.conn, context.out)
	}

	if err != nil {
		handleDriverError(err)
		if context.exitOnError {
			os.Exit(1)
		}
	}
	duration := time.Since(start)

	// output the meta data
	if meta != nil {
		if meta.RowCount == 1 {
			context.WriteString("(1 row)\n")
		} else {
			context.WriteString(fmt.Sprintf("(%d rows)\n", meta.RowCount))
		}
		context.WriteString(fmt.Sprintf("\nsql:%0.3f opt:%0.3f run:%0.3f clk:%s\n", float32(meta.SqlTime)/1000, float32(meta.OptTime)/1000, float32(meta.RunTime)/1000, duration))

	} else if context.timing {
		context.WriteString(fmt.Sprintf("\nclk:%s\n", duration))
	}
}

// Tracks the state of our statement parsing
type state struct {
	context *Context

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
func (s *state) add(line string) (bool, string) {
	escape := s.escape
	for i, c := range line {
		if escape {
			escape = false
			s.escape = false
			continue
		}
		if c == '\\' {
			// if the first character is \
			// and we aren't in a literal
			// than this is a command embedded in the SQL, which we'll allow (like psql)
			if i == 0 && s.literal == 0 {
				command(s.context, strings.TrimRight(line, "\n"))
				return false, ""
			}
			escape = true
			s.escape = true
			continue
		}

		if c == ';' && s.literal == 0 {
			// We have a full statement statement. Add the line up to and including
			// (thus the +1) semi colon
			// TODO: figure out what to do with the rest
			s.WriteString(line[:i+1])
			return true, strings.TrimSpace(line[i+1:])
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
	return false, ""
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

func conditionallyExecuteAndExit(cArg string, fArg string, context *Context) {
	var input string
	if cArg != "" {
		input = strings.TrimSpace(cArg)
	} else if fArg != "" {
		data, err := ioutil.ReadFile(fArg)
		if err != nil {
			log.WithFields(log.Fields{"context": "fArg read"}).Fatal(err)
		}
		input = string(data)
	} else {
		fi, err := os.Stdin.Stat()
		if err != nil {
			log.WithFields(log.Fields{"context": "stat stdin"}).Error(err)
		} else if fi.Mode()&os.ModeNamedPipe != 0 {
			data, err := ioutil.ReadAll(os.Stdin)
			if err != nil {
				log.WithFields(log.Fields{"context": "stdin read"}).Fatal(err)
			}
			input = string(data)
		}
	}

	// no -c, -f or stdin
	if input == "" {
		return
	}

	queries := strings.Split(input, ";")
	for i, q := range queries {
		if q == "" {
			continue
		}
		if !strings.HasSuffix(q, ";") {
			q += ";"
		}
		if i > 0 {
			context.WriteString("\n")
		}
		query(context, q)
	}
	os.Exit(0)
}
