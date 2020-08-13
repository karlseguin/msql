package commands

type Help struct {
}

func (cmd Help) Execute(context Context, input string) {
	context.WriteString(`
\q - Quits the shell
\? - Outputs this help screen
\h - Alias for \?

\f FORMAT - sets the output format to one of: 'raw', 'expanded' or 'sql'
\x on|off - turns expanded format on or off (for compatibility with psql)
\timing on|off - turns timing information on or off
`)
}
