package outputs

import (
	"github.com/karlseguin/msql/driver"
)

func Trash(conn driver.Conn) error {
	for {
		_, fin, err := conn.ReadFrame()
		if err != nil {
			return err
		}
		if fin {
			return nil
		}
	}
}
