package redis

import (
    "log"
    "fmt"
    "errors"
    "strconv"
)

const (
    ServerStateClose = 0
    ServerStateOpen  = 1
)

var (
    minConnNum  = 2
    maxConnNum  = 4
    idleConnNum = 3
)


func MaxConnNum() int {
    return maxConnNum
}

func SetMaxConnNum(num int) {
    if num >= idleConnNum {
        maxConnNum = num
    }
}

func IdleConnNum() int {
    return idleConnNum
}

func SetIdleConnNum(num int) {
    if num >= minConnNum && num <= maxConnNum {
        idleConnNum = num
    }
}

func MinConnNum() int {
    return minConnNum
}

func SetMinConnNum(num int) {
    if num >= 0 && num <= idleConnNum {
        minConnNum = num
    }
}

type Redis struct {
    state int
    addr string
    conns chan *rconn
    connHolder chan bool
}

func New(ip string, port int) (*Redis, error) {
    r := &Redis{
        ServerStateOpen,
        fmt.Sprintf("%s:%d", ip, port),
        make(chan *rconn, idleConnNum),
        make(chan bool, maxConnNum),
    }

    for i := 0; i < minConnNum; i++ {
        if conn, err := newRconn(r.addr); err == nil{
            r.connHolder <- true
            r.conns <- conn
        }
    }

    if len(r.conns) == 0 {
        return nil, errors.New("redis: cannot connect to server")
    }

    return r, nil
}

func (r *Redis) fetchConn() *rconn {
    select {
    case conn := <-r.conns:
        return conn
    default:
        select {
        case conn := <-r.conns:
            return conn
        case r.connHolder <- true:
            conn, err := newRconn(r.addr)
            if err != nil {
                <-r.connHolder
            }
            return conn
        }
    }
}

func (r *Redis) releaseConn(conn *rconn) {
    if conn.state == ConnStateOpen {
        select {
        case r.conns <- conn:
        default:
            r.closeConn(conn)
        }
    }
}

func (r *Redis) closeConn(conn *rconn) {
    if conn.state == ConnStateOpen {
        conn.Close()
        <-r.connHolder
    }
}

func (r *Redis) Exec(cmd string) ([]string, error) {
    conn := r.fetchConn()
    defer r.releaseConn(conn)
    err := conn.write([]byte(cmd + "\r\n"))
    if err != nil {
        r.closeConn(conn)
        return nil, err
    }

    resp, err := conn.readLine()
    if err != nil {
        r.closeConn(conn)
        return nil, err
    }

    switch resp[0] {
    case '+':
        return []string{string(resp[1:])}, nil

    case ':':
        return []string{string(resp[1:])}, nil

    case '$':
        resp, err := conn.readLine()
        if err != nil {
            r.closeConn(conn)
            return nil, err
        }
        return []string{string(resp)}, nil

    case '*':
        num, err := strconv.ParseInt(string(resp[1:]), 10, 32)
        if err != nil {
            return nil, errors.New("redis: bad response")
        }

        rets := make([]string, 0, num)
        for i := 0; i < 2 * int(num); i++ {
            resp, err := conn.readLine()
            if err != nil {
                r.closeConn(conn)
                return nil, err
            }
            if resp[0] == '$' {
                continue
            }
            rets = append(rets, string(resp))
        }
        return rets, nil

    case '-':
        return nil, errors.New("redis: " + string(resp[1:]))
    }

    return nil, errors.New("redis: bad response")
}

func (r *Redis) Set(key string, val interface{}) error {
    return r.SetExtra(key, val, 0, 0, "")
}

func (r *Redis) String(key string) string {
    rets, err := r.Exec("GET " + key)
    if err == nil && len(rets) > 0 {
        return rets[0]
    }
    return ""
}

func (r *Redis) Int(key string) int64 {
    ret := r.String(key)
    num, _ := strconv.ParseInt(ret, 10, 64)
    return num
}

func (r *Redis) Push(side bool, key string, vals ...interface{}) error {
    var ph string
    for i := 0; i < len(vals); i++ {
        ph = ph + " %v"
    }

    var pre string
    if side {
        pre = "R"
    } else {
        pre = "L"
    }

    cmd := fmt.Sprintf("%sPUSH %s %s", pre, key, ph)
    _, err := r.Exec(fmt.Sprintf(cmd, vals...))
    return err
}

func (r *Redis) RPush(key string, vals ...interface{}) error {
    return r.Push(true, key, vals...)
}

func (r *Redis) LPush(key string, vals ...interface{}) error {
    return r.Push(false, key, vals...)
}

func (r *Redis) SetExtra(key string, val interface{}, ex, px int64, cond string) error {
    cmd := fmt.Sprintf("SET %s %v ", key, val)
    if ex > 0 {
        cmd = fmt.Sprintf("%s EX %d", cmd, ex)
    }
    if px > 0 {
        cmd = fmt.Sprintf("%s PX %d", cmd, px)
    }
    if cond == "NX" || cond == "XX" {
        cmd = cmd + " " + cond
    }
    _, err := r.Exec(cmd)
    return err
}

func Test() {
    redis, _ := New("127.0.0.1", 6379)

    redis.Set("key3", 34323523)
    ret := redis.String("key3")
    log.Print(ret)

    num := redis.Int("key3")
    log.Print(num)

    redis.LPush("list5", 4, 5, 6, 6, "aaa")
}
