package commands

type Context interface {
	Output([]byte)
	FormatRaw()
	FormatSQL()
	FormatExpanded()
}
