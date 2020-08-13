package commands

type Context interface {
	WriteString(string)
	Format(string)
	Timing(bool)
}
