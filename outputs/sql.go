package outputs

import (
	"fmt"
	"io"

	"github.com/karlseguin/msql/driver"
	"github.com/olekukonko/tablewriter"
)

// We want to stream data as it's received (one frame at a time), but our table
// renderer doesn't support streaming.
// Our problem is with column widths. The table renderer takes all the data and
// figures out the correct width. That won't work if the data comes in chunks.
// Thankfully, monetdb server returns the max length of each column in its header.
// With this information we can:
// 1 - Render each frame as a distinct table
// 2 - Only render the header on the first table
// 3 - Pad the first row of each frame to the max width to generate a consistent
//     layout across the table renders.
func SQL(conn driver.Conn, out io.Writer) (*driver.Meta, error) {
	result, err := conn.ReadResult()
	if err != nil {
		return nil, err
	}

	meta := result.Meta()
	if ok, data := result.IsSimple(); ok {
		io.WriteString(out, data)
		return meta, nil
	}

	// We need to pad to whichever is wider: the column width or value widths
	columns := result.Columns()
	lengths := result.Lengths()
	for i, length := range lengths {
		columnLength := len(columns[i])
		if columnLength > length {
			lengths[i] = columnLength
		}
	}
	fmt.Println(lengths)

	_, err = renderSQLPage(result, lengths, true, out)
	if err != nil {
		return nil, err
	}

	for {
		more, err := renderSQLPage(result, lengths, false, out)
		if err != nil {
			return meta, err
		}
		if !more {
			break
		}
	}

	return meta, nil
}

func renderSQLPage(result driver.Result, lengths []int, showHeaders bool, out io.Writer) (bool, error) {
	data, err := result.Next()
	if err != nil {
		return false, err
	}

	if data == nil {
		return false, nil
	}

	first := data[0]
	for i, d := range first {
		first[i] = tablewriter.PadRight(d, " ", lengths[i])
	}
	data[0] = first

	table := tablewriter.NewWriter(out)
	table.SetAutoFormatHeaders(false)
	table.SetColWidth(72)
	table.SetHeaderLine(true)
	table.SetAutoWrapText(false)
	table.SetReflowDuringAutoWrap(false)
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	table.SetBorders(tablewriter.Border{Left: false, Top: false, Right: false, Bottom: false})
	table.SetCenterSeparator("|")

	if showHeaders {
		table.SetHeader(result.Columns())
	}

	table.AppendBulk(data)
	table.Render()

	return true, nil
}
