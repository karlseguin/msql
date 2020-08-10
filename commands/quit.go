package commands

import (
	"os"
)

type Quit struct {
}

func (cmd Quit) Execute(context Context, input string) {
	os.Exit(0)
}
