package redis

import (
    "net"
    "time"
    "bufio"
    "errors"
    "strconv"
)

const (
    ConnStateClose = 0
    ConnStateOpen  = 1
)

type rconn struct {
    state int
    db int
    conn net.Conn
    reader *bufio.Reader
}


func newRconn(addr string, db int) (*rconn, error) {
    conn, err := net.Dial("tcp", addr)
    if err == nil{
        conn.SetDeadline(time.Time{})
        return &rconn{ConnStateOpen, db, conn, bufio.NewReader(conn)}, nil
    }

    return nil, err
}

func (c *rconn) resp(cmd string) ([]string, error) {
    err := c.write([]byte(cmd + "\r\n"))
    if err != nil {
        return nil, err
    }
    ret, err := c.readLine()
    if err != nil {
        return nil, err
    }

    switch ret[0] {
    case '+':
        return []string{string(ret[1:])}, nil
    case '$':
        num, err := strconv.ParseInt(string(ret[1:]), 10, 32)
        if err != nil {
            return nil, errors.New("redis: bad response")
        }
        if num < 1 {
            return []string{""}, nil
        }
        ret, err := c.readLine()
        if err != nil {
            return nil, err
        }
        return []string{string(ret)}, nil
    case ':':
        return []string{string(ret[1:])}, nil
    case '*':
        num, err := strconv.ParseInt(string(ret[1:]), 10, 32)
        if err != nil {
            return nil, errors.New("redis: bad response")
        }

        rets := make([]string, 0, num)
        for i := 0; i < 2 * int(num); i++ {
            ret, err := c.readLine()
            if err != nil {
                return nil, err
            }
            if ret[0] == '$' {
                continue
            }
            rets = append(rets, string(ret))
        }
        return rets, nil
    case '-':
        return nil, errors.New("redis: " + string(ret[1:]))
    }
    return nil, errors.New("redis: bad response")
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
