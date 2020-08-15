package main

import (
	"io/ioutil"
	"os"
	"path"
	"strings"
	"unicode"

	log "github.com/sirupsen/logrus"
)

type Preferences struct {
	historyFile  string
	passwordFile string
	prompt       string
	timing       bool
}

func loadPreferences() Preferences {
	defaultPrompt := "${host}@${database} => "
	userConfigDir, err := os.UserConfigDir()
	if err != nil {
		log.WithFields(log.Fields{"context": "failed to load config dir"}).Error(err)
		return Preferences{prompt: defaultPrompt}
	}

	configDir := path.Join(userConfigDir, "msql")
	os.Mkdir(configDir, 0750)
	configFile := path.Join(configDir, "config")

	preferences := Preferences{
		historyFile:  path.Join(configDir, "history"),
		passwordFile: path.Join(configDir, ".pass"),
		prompt:       defaultPrompt,
	}

	file, err := ioutil.ReadFile(configFile)
	if err != nil {
		if os.IsNotExist(err) {
			log.WithFields(log.Fields{"context": configFile}).Info("no preference file")
		} else {
			log.WithFields(log.Fields{"context": "read preference file", "path": configFile}).Error(err)
		}
		return preferences
	}

	lines := strings.Split(string(file), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || line[0] == '#' {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			log.WithFields(log.Fields{"context": configFile, "line": line}).Info("invalid property")
			continue
		}

		value := stripComment(parts[1])
		switch parts[0] {
		case "historyFile":
			preferences.historyFile = value
			break
		case "passwordFile":
			preferences.passwordFile = value
			break
		case "timing":
			value = strings.ToLower(value)
			preferences.timing = value == "on" || value == "1" || value == "true"
			break
		case "prompt":
			preferences.prompt = strings.Trim(value, "\"")
			break
		default:
			log.WithFields(log.Fields{"context": configFile, "key": parts[0]}).Info("unknwon preference key")
		}
	}

	return preferences
}

func stripComment(source string) string {
	if cut := strings.IndexAny(source, "#"); cut >= 0 {
		return strings.TrimRightFunc(source[:cut], unicode.IsSpace)
	}
	return source
}
