# MonetDB CLI

A CLI tool for working with MonetDB. Meant to supplement the built-in mclient tool.

Main features are:

1. Support for pgpass
2. Multi-line history
3. Small Quality of Life improvements

The biggest thing missing right now, as far as I'm concerned is support for \d+ TABLE.

## Installation

You need a relatively recent version of [Go installed](https://golang.org/dl/), then:

```
go get github.com/karlseguin/msql
go build github.com/karlseguin/msql
```

And copy the generate `msql` binary to `/usr/local/bin` (or some other place in your PATH).

## Configuration
msql stores its state in `$XDG_CONFIG_HOME/msql` or `$HOME/.config/msql`. There are three files by default: `config`, `history` and `.pass`.

The `config` contains 1 `setting=value` per line. The following settings are currently supported along with their default value:

```
timing=off
prompt="${host}@${database} => "
historyFile=$XDG_CONFIG_HOME/msql/history
passwordFILE=$XDG_CONFIG_HOME/msql/.pass
```

When `timing` is `on` additional timing information is shown after each query.

`prompt` supports the following variables: `${user}`, `${role}`, `${schema}`, `${host}`, `${port}` and  `${database}`.

`historyFile` supports the same variables as `prompt`. To have a distinct history file per host+database, you could do: `historyFile=/home/karl/.config/msql/history.${host}@${database}`.

`passwordFile` points to a file that matches the format of .pgpass. You can point this to your .pgpass file if you want  (e.g.: `/home/karl/.pgpass`).



`$XDG_CONFIG_HOME/msql/config` is a key value list of settings
