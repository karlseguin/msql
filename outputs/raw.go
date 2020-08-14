package outputs

import (
	"io"

	"github.com/karlseguin/msql/driver"
)

func Raw(conn driver.Conn, out io.Writer) (*driver.Meta, error) {
	data, fin, err := conn.ReadFrame()
	meta := driver.NewMeta(data)
	for {
		if err != nil {
			return nil, err
		}
		out.Write(data)
		if fin {
			return meta, nil
		}
		data, fin, err = conn.ReadFrame()
	}
}
