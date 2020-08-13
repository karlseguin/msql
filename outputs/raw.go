package outputs

import (
	"io"

	"github.com/karlseguin/msql/driver"
)

func Raw(conn driver.Conn, out io.Writer) error {
	for {
		data, fin, err := conn.ReadFrame()
		if err != nil {
			return err
		}
		out.Write(data)
		if fin {
			out.Write([]byte("\n"))
			return nil
		}
	}
}
