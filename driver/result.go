package driver

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"
)

type Result interface {
	Columns() []string
	Lengths() []int
	Meta() *Meta
	Next() ([][]string, error)
	IsSimple() (bool, string)
}

type Meta struct {
	RowCount int
	SqlTime  int
	OptTime  int
	RunTime  int
}

func newResult(c Conn) (Result, error) {
	data, fin, err := c.ReadFrame()
	if err != nil {
		return nil, err
	}

	if len(data) > 0 && data[0] == '!' {
		return nil, monetDBError(string(data[1:]))
	}

	if len(data) == 0 {
		return EmptyResult{}, nil
	}

	if bytes.HasPrefix(data, []byte("&1 ")) {
		return newQueryResult(c, data, fin)
	}

	if bytes.HasPrefix(data, []byte("&2 ")) {
		parts := bytes.SplitN(data[3:], []byte(" "), 2)
		if len(parts) == 2 {
			n, err := strconv.Atoi(string(parts[0]))
			if err != nil {
				return nil, detailedDriverError("invalid upsert response", string(data))
			}
			return AffectedResult{affected: n}, nil
		}
	}

	if bytes.HasPrefix(data, []byte("&3 ")) {
		return OKResult{}, nil
	}

	return nil, detailedDriverError("unknown response", string(data))
}

type SimpleResult struct{}

func (r SimpleResult) Meta() *Meta               { return nil }
func (r SimpleResult) Lengths() []int            { return nil }
func (r SimpleResult) Columns() []string         { return nil }
func (r SimpleResult) Next() ([][]string, error) { return nil, nil }

type EmptyResult struct{ SimpleResult }

func (r EmptyResult) IsSimple() (bool, string) { return true, "" }

type OKResult struct{ SimpleResult }

func (r OKResult) IsSimple() (bool, string) { return true, "OK\n\n" }

type AffectedResult struct {
	affected int
	SimpleResult
}

func (r AffectedResult) IsSimple() (bool, string) {
	if r.affected == 1 {
		return true, "1 row affected\n\n"
	}
	return true, fmt.Sprintf("%d rows affected\n\n", r.affected)
}

// TODO: this should probably be an interface that can return data based on the
// type of result.
type QueryResult struct {
	conn    Conn
	fin     bool
	lengths []int
	columns []string
	meta    *Meta
	buffer  bytes.Buffer
}

func newQueryResult(c Conn, data []byte, fin bool) (Result, error) {
	parts := bytes.SplitN(data, []byte("\n"), 6)

	metaLine := string(parts[0])
	metaStrings := strings.Split(metaLine, " ")

	var meta *Meta
	if len(metaStrings) == 9 {
		meta = new(Meta)
		rowCount, err := strconv.Atoi(metaStrings[2])
		if err != nil {
			return nil, detailedDriverError("invalid query result (1)", metaLine)
		}
		meta.RowCount = rowCount
		if meta.RunTime, err = extractTiming(metaStrings[6]); err != nil {
			return nil, detailedDriverError("invalid query result (2)", metaLine)
		}
		if meta.OptTime, err = extractTiming(metaStrings[7]); err != nil {
			return nil, detailedDriverError("invalid query result (3)", metaLine)
		}
		if meta.SqlTime, err = extractTiming(metaStrings[8]); err != nil {
			return nil, detailedDriverError("invalid query result (4)", metaLine)
		}
	}

	columnLine := string(parts[2])
	columnLine = columnLine[2 : len(columnLine)-len(" # name")]
	columns := strings.Split(columnLine, ",\t")

	lengthLine := string(parts[4])
	lengthLine = lengthLine[2 : len(lengthLine)-len(" # length")]
	lengthStrings := strings.Split(lengthLine, ",\t")

	lengths := make([]int, len(lengthStrings))
	for i, l := range lengthStrings {
		width, _ := strconv.Atoi(l)
		lengths[i] = width
	}

	var buffer bytes.Buffer
	buffer.Write(parts[5])

	return &QueryResult{
		conn:    c,
		fin:     fin,
		columns: columns,
		lengths: lengths,
		buffer:  buffer,
		meta:    meta,
	}, nil
}

func (r *QueryResult) IsSimple() (bool, string) { return false, "" }

func (r *QueryResult) Columns() []string {
	return r.columns
}

func (r *QueryResult) Lengths() []int {
	return r.lengths
}

func (r *QueryResult) Meta() *Meta {
	return r.meta
}

func (r *QueryResult) Next() ([][]string, error) {
	if r.fin {
		if r.buffer.Len() == 0 {
			return nil, nil
		}
		return r.asRows(), nil
	}

	data, fin, err := r.conn.ReadFrame()
	if err != nil {
		return nil, err
	}

	r.fin = fin
	r.buffer.Write(data)
	rows := r.asRows()
	if len(rows) == 0 {
		if r.fin {
			return nil, nil
		}
		return r.Next()
	}
	return rows, nil
}

func (r *QueryResult) asRows() [][]string {
	data := r.buffer.Bytes()
	rows := bytes.Split(data, []byte("\n"))

	var partialRow []byte
	lastRowIndex := len(rows) - 1
	// The last row isn't a complete row. We have to ignore it in this call to
	// asRows, but we append it to our buffer for it to be merged with data from
	// the next ReadFrame (on the next call to our Next() function above)
	if !bytes.HasSuffix(rows[lastRowIndex], []byte("\t]")) {
		partialRow = rows[lastRowIndex]
		rows = rows[0:lastRowIndex]
	}

	table := make([][]string, len(rows))
	for i, row := range rows {
		// 2 : len()-2   to strip out the leading and trailing '[\t' and '\t]'
		values := strings.Split(string(row[2:len(row)-2]), ",\t")
		for i, value := range values {
			if value[0] == '"' {
				values[i] = unquote(strings.Trim(value, "\""))
			}
		}
		table[i] = values
	}

	r.buffer.Reset()
	if partialRow != nil {
		r.buffer.Write(partialRow)
	}

	return table
}

// taken from https://github.com/fajran/go-monetdb/blob/master/converter.go
func unquote(s string) string {
	if !strings.Contains(s, "\\") {
		return s
	}

	var runeTmp [utf8.UTFMax]byte
	buf := make([]byte, 0, 3*len(s)/2) // Try to avoid more allocations.
	for len(s) > 0 {
		c, multibyte, ss, err := strconv.UnquoteChar(s, '\'')
		if err != nil {
			fmt.Printf("E: %v\n -> %s\n", err, s)
			return s
		}
		s = ss
		if c < utf8.RuneSelf || !multibyte {
			buf = append(buf, byte(c))
		} else {
			n := utf8.EncodeRune(runeTmp[:], c)
			buf = append(buf, runeTmp[:n]...)
		}
	}
	return string(buf)
}

func extractTiming(value string) (int, error) {
	n, err := strconv.Atoi(value)
	if err != nil {
		return 0, err
	}
	return n, nil
}
