package driver

import (
	"bytes"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/binary"
	"encoding/hex"
	"hash"
	"io"
	"net"
	"net/url"
	"strings"
	"time"
)

type Conn struct {
	net.Conn
	buffer  []byte
	scratch []byte
}

func Open(config Config) (Conn, error) {
	socket, err := net.DialTimeout("tcp", config.Host, time.Second*5)
	if err != nil {
		return Conn{}, err
	}

	c := Conn{
		Conn:    socket,
		scratch: make([]byte, 2),
		buffer:  make([]byte, 8192), // 8190 max frame size + 2 for header
	}

	redirect, err := c.authenticate(config, 0)
	if err != nil {
		socket.Close()
		return Conn{}, err
	}
	if redirect != nil {
		socket.Close()
		config.Host = redirect.Host
		return Open(config)
	}

	c.SetDeadline(time.Now().Add(time.Second * 5))
	if err := c.configure(config); err != nil {
		socket.Close()
		return Conn{}, err
	}

	if err := c.disableReplySize(); err != nil {
		socket.Close()
		return Conn{}, err
	}

	c.SetDeadline(time.Time{})
	return c, nil
}

func (c Conn) authenticate(config Config, tries uint8) (*url.URL, error) {
	if tries == 10 {
		return nil, driverError("too many proxy login iterations")
	}

	c.SetDeadline(time.Now().Add(time.Second * 10))
	challenge, err := c.readMessageString()
	if err != nil {
		return nil, err
	}
	parts := strings.Split(string(challenge), ":")
	if len(parts) != 7 {
		return nil, detailedDriverError("invalid challenge response", string(challenge))
	}
	if parts[2] != "9" {
		return nil, detailedDriverError("invalid challenge version", parts[2])
	}

	salt := parts[0]

	authHasher, authName := parseAuthType(parts[3])
	if authHasher == nil {
		return nil, detailedDriverError("no supported auth types", parts[3])
	}

	algoHasher := parseHashAlgo(parts[5])
	if algoHasher == nil {
		return nil, detailedDriverError("unsupported hash algorithm", parts[5])
	}

	algoHasher.Write([]byte(config.Password))
	password := hex.EncodeToString(algoHasher.Sum(nil))

	authHasher.Write([]byte(password))
	authHasher.Write([]byte(salt))
	digest := hex.EncodeToString(authHasher.Sum(nil))

	err = c.Send("LIT:", config.UserName, ":", authName, digest, ":sql:", config.Database, ":")
	if err != nil {
		return nil, err
	}

	reply, err := c.readMessageString()
	if err != nil {
		return nil, err
	}

	if reply == "" {
		// success
		return nil, nil
	}

	if strings.HasPrefix(reply, "^mapi:merovingian:") {
		return c.authenticate(config, tries+1)
	}

	if strings.HasPrefix(reply, "^mapi:") {
		u := strings.SplitN(reply, "\n", 1)[0]

		// 1 - len(u) -1 to strip out the leading ^mapi:  and the trailing \n
		url, err := url.Parse(u[6 : len(u)-1])
		if err != nil {
			return nil, detailedDriverError("invalid login redirect", reply)
		}
		return url, nil
	}

	return nil, detailedDriverError("invalid login response", reply)
}

func (c Conn) configure(config Config) error {
	if config.Schema != "" {
		if err := c.set("schema", config.Schema); err != nil {
			return err
		}
	}

	if config.Role != "" {
		if err := c.set("role", config.Role); err != nil {
			return err
		}
	}
	return nil
}

func (c Conn) set(field string, value string) error {
	if err := c.Send("sset  ", field, " ", value, ";"); err != nil {
		return err
	}
	data, err := c.readMessage()
	if err != nil {
		return err
	}
	if !bytes.HasPrefix(data, []byte("&3 ")) {
		return detailedDriverError("invalid response to SET command", string(data))
	}
	return nil
}

func (c Conn) disableReplySize() error {
	if err := c.Send("Xreply_size -1\n"); err != nil {
		return err
	}
	if _, err := c.readMessage(); err != nil {
		return err
	}
	return nil
}

func (c Conn) ReadResult() (Result, error) {
	return newResult(c)
}

func (c Conn) readMessageString() (string, error) {
	data, err := c.readMessage()
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func (c Conn) readMessage() ([]byte, error) {
	var buffer bytes.Buffer
	for {
		data, fin, err := c.ReadFrame()
		if err != nil {
			return nil, err
		}
		buffer.Write(data)
		if fin {
			break
		}
	}

	message := buffer.Bytes()
	if len(message) > 0 && message[0] == '!' {
		return nil, monetDBError(string(message[1:]))
	}
	return message, nil
}

// Exposed since we want to be able to "stream" the output using various
// transformers (raw, expanded, ...)
// Note that the buffer remains owned by the connection - callers should
// copy it if they need it to survive the next call to ReadFrame (or any other
// calls involving the connection)
func (c Conn) ReadFrame() ([]byte, bool, error) {
	scratch, err := c.readN(c.scratch)
	if err != nil {
		return nil, false, networkError(err)
	}

	header := binary.LittleEndian.Uint16(scratch)
	fin := header & 1
	len := header >> 1

	data, err := c.readN(c.buffer[:len])
	if err != nil {
		return nil, false, networkError(err)
	}
	return data, fin == 1, nil
}

func (c Conn) readN(data []byte) ([]byte, error) {
	_, err := io.ReadFull(c.Conn, data)
	return data, err
}

func (c Conn) Send(parts ...string) error {
	l := 0
	for _, part := range parts {
		l += len(part)
	}

	if l > 8190 {
		return c.multiFrameSend(l, parts)
	}

	// optimize a little since this is overwhelmingly the common case
	scratch := c.scratch
	binary.LittleEndian.PutUint16(scratch, uint16(l<<1|1))
	if _, err := c.Write(scratch); err != nil {
		return nil
	}
	for _, part := range parts {
		if _, err := c.Write([]byte(part)); err != nil {
			return networkError(err)
		}
	}

	return nil
}

func (c Conn) multiFrameSend(l int, parts []string) error {
	var buffer strings.Builder
	buffer.Grow(l)
	for _, part := range parts {
		buffer.WriteString(part)
	}

	data := []byte(buffer.String())
	for {
		if len(data) > 8190 {
			// max-length + non-fin
			if _, err := c.Write([]byte{252, 63}); err != nil {
				return err
			}
			if _, err := c.Write(data[0:8190]); err != nil {
				return err
			}
			data = data[8190:]
		} else {
			scratch := c.scratch
			binary.LittleEndian.PutUint16(scratch, uint16(len(data)<<1|1))
			if _, err := c.Write(scratch); err != nil {
				return err
			}
			if _, err := c.Write(data); err != nil {
				return err
			}
			// we're done
			return nil
		}
	}
}

func contains(haystack []string, needle string) bool {
	for _, v := range haystack {
		if needle == v {
			return true
		}
	}
	return false
}

func parseAuthType(list string) (hash.Hash, string) {
	auth_types := strings.Split(list, ",")
	if contains(auth_types, "SHA512") {
		return sha512.New(), "{SHA512}"
	}

	if contains(auth_types, "SHA256") {
		return sha256.New(), "{SHA256}"
	}

	if contains(auth_types, "SHA224") {
		return sha256.New224(), "{SHA224}"
	}

	return nil, ""
}

func parseHashAlgo(algo string) hash.Hash {
	switch algo {
	case "SHA512":
		return sha512.New()
	case "SHA256":
		return sha256.New()
	case "SHA384":
		return sha512.New384()
	case "SHA224":
		return sha256.New224()
	default:
		return nil
	}
}
