package outputs

import (
	"io"
	"strconv"

	"github.com/karlseguin/msql/driver"
)

func Expanded(conn driver.Conn, out io.Writer) (*driver.Meta, error) {
	result, err := conn.ReadResult()
	if err != nil {
		return nil, err
	}

	meta := result.Meta()
	if ok, data := result.IsSimple(); ok {
		io.WriteString(out, data)
		return meta, nil
	}

	columns := make([][]byte, len(result.Columns()))
	for i, column := range result.Columns() {
		columns[i] = []byte("\n" + column + " | ")
	}

	rowIndex := 1
	for {
		rows, err := result.Next()
		if err != nil {
			return nil, err
		}
		if rows == nil {
			return meta, nil
		}

		for _, row := range rows {
			io.WriteString(out, "-[ RECORD ")
			io.WriteString(out, strconv.Itoa(rowIndex))
			out.Write([]byte(" ] "))
			for colIndex, column := range columns {
				out.Write(column)
				io.WriteString(out, row[colIndex])
			}
			out.Write([]byte("\n"))
			rowIndex += 1
		}
	}
}
