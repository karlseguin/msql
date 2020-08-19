package driver

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
)

type Result interface {
	Types() []string
	Columns() []string
	Lengths() []int
	Meta() *Meta
	Next() ([][]string, error)
	IsSimple() (bool, string)
	Rows() ([][]string, error)
	Maps() ([]map[string]string, error)
}

type Meta struct {
	RowCount int
	SqlTime  int
	OptTime  int
	RunTime  int
}

// exposed this way so that all outputs (including raw and trash) can use it
func NewMeta(data []byte) *Meta {
	type Indexes struct {
		max      int
		rowCount int
		runTime  int
		optTime  int
		sqlTime  int
	}

	var indexes *Indexes
	if bytes.HasPrefix(data, []byte("&1 ")) {
		indexes = &Indexes{9, 2, 6, 7, 8}
	} else if bytes.HasPrefix(data, []byte("&2 ")) {
		indexes = &Indexes{7, 1, 4, 5, 6}
	} else {
		return nil // some response have no meta
	}

	parts := bytes.SplitN(data, []byte(" "), indexes.max)
	if len(parts) < indexes.max {
		return nil
	}

	meta := new(Meta)
	meta.RowCount, _ = strconv.Atoi(string(parts[indexes.rowCount]))
	meta.RunTime, _ = strconv.Atoi(string(parts[indexes.runTime]))
	meta.OptTime, _ = strconv.Atoi(string(parts[indexes.optTime]))
	meta.SqlTime, _ = strconv.Atoi(string(parts[indexes.sqlTime]))

	return meta
}

func newResult(c Conn) (Result, error) {
	data, fin, err := c.ReadFrame()
	if err != nil {
		return nil, err
	}

	if len(data) > 0 && data[0] == '!' {
		// strip out leading ! and trailing \n
		return nil, monetDBError(string(data[1 : len(data)-1]))
	}

	if len(data) == 0 {
		return EmptyResult{}, nil
	}

	if bytes.HasPrefix(data, []byte("&3 ")) {
		return OKResult{}, nil
	}

	meta := NewMeta(data)

	if bytes.HasPrefix(data, []byte("&1 ")) {
		return newQueryResult(c, data, fin, meta)
	}

	if bytes.HasPrefix(data, []byte("&2 ")) {
		return OKResult{SimpleResult: SimpleResult{meta: meta}}, nil
	}

	if bytes.HasPrefix(data, []byte("&5 ")) {
		parts := bytes.SplitN(data[3:], []byte(" "), 2)
		return PrepareResult{
			id:           string(parts[0]),
			SimpleResult: SimpleResult{meta: meta},
		}, nil
	}

	return nil, detailedDriverError("unknown response", string(data))
}

type SimpleResult struct {
	meta *Meta
}

func (r SimpleResult) Meta() *Meta                        { return r.meta }
func (_ SimpleResult) Types() []string                    { return nil }
func (_ SimpleResult) Lengths() []int                     { return nil }
func (_ SimpleResult) Columns() []string                  { return nil }
func (_ SimpleResult) Next() ([][]string, error)          { return nil, nil }
func (_ SimpleResult) Rows() ([][]string, error)          { return nil, nil }
func (_ SimpleResult) Maps() ([]map[string]string, error) { return nil, nil }

type EmptyResult struct{ SimpleResult }

func (r EmptyResult) IsSimple() (bool, string) { return true, "" }

type OKResult struct{ SimpleResult }

func (r OKResult) IsSimple() (bool, string) { return true, "OK\n" }

type PrepareResult struct {
	id string
	SimpleResult
}

func (r PrepareResult) IsSimple() (bool, string) {
	return true, fmt.Sprintf("OK, use: exec %s(...);\n", r.id)
}

// TODO: this should probably be an interface that can return data based on the
// type of result.
type QueryResult struct {
	conn    Conn
	fin     bool
	lengths []int
	scratch []byte
	types   []string
	columns []string
	meta    *Meta
	buffer  bytes.Buffer
}

func newQueryResult(c Conn, data []byte, fin bool, meta *Meta) (Result, error) {
	parts := bytes.SplitN(data, []byte("\n"), 6)

	columnLine := string(parts[2])
	columnLine = columnLine[2 : len(columnLine)-len(" # name")]
	columns := strings.Split(columnLine, ",\t")

	typeLine := string(parts[3])
	typeLine = typeLine[2 : len(typeLine)-len(" # type")]
	types := strings.Split(typeLine, ",\t")

	lengthLine := string(parts[4])
	lengthLine = lengthLine[2 : len(lengthLine)-len(" # length")]
	lengthStrings := strings.Split(lengthLine, ",\t")

	max := 0
	lengths := make([]int, len(lengthStrings))
	for i, l := range lengthStrings {
		width, _ := strconv.Atoi(l)
		lengths[i] = width
		if width > max {
			max = width
		}
	}
	// try to reduce the amount we'll need to allocate for unquoting the strings
	scratch := make([]byte, 3*max/2)

	var buffer bytes.Buffer
	buffer.Write(parts[5])

	return &QueryResult{
		conn:    c,
		fin:     fin,
		types:   types,
		columns: columns,
		lengths: lengths,
		buffer:  buffer,
		meta:    meta,
		scratch: scratch,
	}, nil
}

func (r *QueryResult) IsSimple() (bool, string) { return false, "" }

func (r *QueryResult) Types() []string {
	return r.types
}

func (r *QueryResult) Columns() []string {
	return r.columns
}

func (r *QueryResult) Lengths() []int {
	return r.lengths
}

func (r *QueryResult) Meta() *Meta {
	return r.meta
}

func (r *QueryResult) Rows() ([][]string, error) {
	rows := make([][]string, 0, r.meta.RowCount)
	for {
		data, err := r.Next()
		if err != nil {
			return nil, err
		}
		if data == nil {
			return rows, nil
		}
		rows = append(rows, data...)
	}
}

func (r *QueryResult) Maps() ([]map[string]string, error) {
	columns := r.columns
	rows := make([]map[string]string, 0, r.meta.RowCount)

	for {
		data, err := r.Next()
		if err != nil {
			return nil, err
		}
		if data == nil {
			return rows, nil
		}
		for _, row := range data {
			m := make(map[string]string, len(columns))
			for i, column := range columns {
				m[column] = row[i]
			}
			rows = append(rows, m)
		}
	}
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
	// a frame could start with the newline from the last row, ignore it
	// else our split will put an empty row at the front
	if data[0] == '\n' {
		data = data[1:]
	}
	rows := bytes.Split(data, []byte("\n"))

	var partialRow []byte
	lastRowIndex := len(rows) - 1
	// The last row isn't a complete row. We have to ignore it in this call to
	// asRows, but we append it to our buffer for it to be merged with data from
	// the next ReadFrame (on the next call to our Next() function above)
	if !bytes.HasSuffix(rows[lastRowIndex], []byte("\t]")) {
		partialRow = rows[lastRowIndex]
		rows = rows[:lastRowIndex]
	}

	table := make([][]string, len(rows))
	for i, row := range rows {
		// 2 : len()-2   to strip out the leading and trailing '[\t' and '\t]'
		values := strings.Split(string(row[2:len(row)-2]), ",\t")
		for i, value := range values {
			if value[0] == '"' {
				values[i] = unquote(strings.Trim(value, "\""), r.scratch[:0])
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

func unquote(s string, buf []byte) string {
	if strings.IndexByte(s, '\\') == -1 {
		return s
	}

	escape := false
	for _, c := range s {
		if escape {
			escape = false
			if c == 'f' {
				buf = append(buf, '\f')
			} else if c == 'n' {
				buf = append(buf, '\n')
			} else if c == 'r' {
				buf = append(buf, '\r')
			} else if c == 't' {
				buf = append(buf, '\t')
			} else if c == 'v' {
				buf = append(buf, '\v')
			} else if c == '\\' {
				buf = append(buf, '\\')
			} else if c == '\'' {
				buf = append(buf, '\'')
			} else if c == '"' {
				buf = append(buf, '"')
			} else {
				buf = append(buf, string(c)...)
			}
		} else if c == '\\' {
			escape = true
		} else {
			buf = append(buf, string(c)...)
		}
	}
	return string(buf)
}
