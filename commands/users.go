package commands

type Users struct {
}

func (cmd Users) Execute(context Context, args string) {
	context.Query("select * from sys.users;")
}
