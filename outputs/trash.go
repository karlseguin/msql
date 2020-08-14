package outputs

import (
	"github.com/karlseguin/msql/driver"
)

func Trash(conn driver.Conn) (*driver.Meta, error) {
	data, fin, err := conn.ReadFrame()
	meta := driver.NewMeta(data)
	for {
		if err != nil {
			return nil, err
		}
		if fin {
			return meta, nil
		}
		_, fin, err = conn.ReadFrame()
	}
}
