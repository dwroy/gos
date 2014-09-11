package redis

import (
    "net"
    "time"
    "bufio"
    "errors"
)

const (
    ConnStateClose = 0
    ConnStateOpen  = 1
)

type rconn struct {
    state int
    conn net.Conn
    reader *bufio.Reader
}


func newRconn(addr string) (*rconn, error) {
    conn, err := net.Dial("tcp", addr)
    if err == nil{
        conn.SetDeadline(time.Time{})
        return &rconn{ConnStateOpen, conn, bufio.NewReader(conn)}, nil
    }

    return nil, err
}

func (c *rconn) readLine() ([]byte, error) {
    line, err := c.reader.ReadSlice('\n')
    if err == bufio.ErrBufferFull {
        return nil, errors.New("redis: bad response title")
    }

    if err != nil {
        return nil, err
    }

    i := len(line) - 2
    if i < 0 || line[i] != '\r' {
        return nil, errors.New("redis: bad response line terminator")
    }

    return line[:i], nil
}

func (c *rconn) write(cmd []byte) error {
    ret, err := c.conn.Write(cmd)
    if ret != len(cmd) {
        return errors.New("redis: cannot send command to server")
    }
    if err != nil {
        return err
    }
    return nil
}

func (c *rconn) Close() error {
    c.state = ConnStateClose
    return c.Close()
}
