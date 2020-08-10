package driver

import (
	"fmt"
)

const (
	DRIVER_ERROR  = "driver"
	MONETDB_ERROR = "monetdb"
	NETWORK_ERROR = "network"
)

type Error struct {
	Source  string
	Message string
	Details string
	Inner   error
}

func (e Error) Error() string {
	if e.Inner != nil {
		return e.Inner.Error()
	}

	if e.Details == "" {
		return fmt.Sprintf("%s - %s", e.Source, e.Message)
	}

	return fmt.Sprintf("%s - %s\n%s", e.Source, e.Message, e.Details)
}

func driverError(message string) Error {
	return detailedDriverError(message, "")
}

func detailedDriverError(message string, details string) Error {
	return Error{
		Source:  DRIVER_ERROR,
		Message: message,
		Details: details,
	}
}

func monetDBError(message string) Error {
	return Error{
		Source:  MONETDB_ERROR,
		Message: message,
	}
}

func networkError(err error) Error {
	return Error{
		Source: NETWORK_ERROR,
		Inner:  err,
	}
}
