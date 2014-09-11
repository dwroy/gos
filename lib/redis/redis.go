package redis

import (
    "log"
    "fmt"
    "errors"
    "strings"
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
    db int
    addr string
    conns chan *rconn
    connSlots chan bool
}

func New(ip string, port, db int) (*Redis, error) {
    r := &Redis{
        ServerStateOpen, db,
        fmt.Sprintf("%s:%d", ip, port),
        make(chan *rconn, idleConnNum),
        make(chan bool, maxConnNum),
    }
    for i := 0; i < minConnNum; i++ {
        if conn, err := newRconn(r.addr, db); err == nil{
            r.connSlots <- true
            r.conns <- conn
        }
    }
    if len(r.conns) == 0 {
        return nil, errors.New("redis: cannot connect to server")
    }
    return r, nil
}

func (r *Redis) fetchConn() (*rconn, error) {
    var conn *rconn
    select {
    case conn = <-r.conns:
        if conn.db != r.db {
            _, err := conn.resp(fmt.Sprintf("SELECT %d", r.db))
            if err != nil {
                r.releaseConn(conn)
                return nil, err
            }
            conn.db = r.db
        }
    default:
        select {
        case conn = <-r.conns:
        case r.connSlots <- true:
            var err error
            conn, err = newRconn(r.addr, r.db)
            if err != nil {
                <-r.connSlots
                return nil, err
            }
        }
    }
    return conn, nil
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
        <-r.connSlots
    }
}

func (r *Redis) SetDB(db int) {
    r.db = db
}

func (r *Redis) Exec(cmd string) ([]string, error) {
    conn, err := r.fetchConn()
    if err != nil {
        return nil, err
    }
    defer r.releaseConn(conn)
    rets, err := conn.resp(cmd)
    if err == nil {
        return rets, nil
    }
    r.closeConn(conn)
    return nil, err
}

func (r *Redis) Set(key string, val interface{}) error {
    return r.SetExtra(key, val, 0, 0, "")
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

func (r *Redis) LPush(key string, vals ...interface{}) int {
    return r.push("LPUSH", key, vals...)
}

func (r *Redis) RPush(key string, vals ...interface{}) int {
    return r.push("RPUSH", key, vals...)
}

func (r *Redis) push(cmd, key string, vals ...interface{}) int {
    slots := make([]string, 0, len(vals))
    for i := 0; i < len(vals); i++ {
        slots = append(slots,  "%v")
    }
    cmd = fmt.Sprintf("%s %s %s", cmd, key, strings.Join(slots, " "))
    rets, err := r.Exec(fmt.Sprintf(cmd, vals...))
    if err == nil && len(rets) > 0 {
        num, _ := strconv.ParseInt(rets[0], 10, 64)
        return int(num)
    }
    return 0
}

func (r *Redis) LRange(key string, start, end int64) []string {
    cmd := fmt.Sprintf("LRANGE %s %d %d", key, start, end)
    rets, _ := r.Exec(cmd)
    return rets
}

func (r *Redis) LPop(key string) string {
    return r.pop("LPOP", key)
}

func (r *Redis) RPop(key string) string {
    return r.pop("RPOP", key)
}

func (r *Redis) pop(cmd, key string) string {
    rets, err := r.Exec(cmd + " " + key)
    if err == nil && len(rets) > 0 {
        return rets[0]
    }
    return ""
}

func (r *Redis) BLPop(key string, sec int) string {
    return r.bpop("BLPOP", key, sec)
}

func (r *Redis) BRPop(key string, sec int) string {
    return r.bpop("BRPOP", key, sec)
}

func (r *Redis) bpop(cmd, key string, sec int) string {
    rets, err := r.Exec(fmt.Sprintf("%s %s %d", cmd, key, sec))
    if err == nil && len(rets) == 2 {
        return rets[1]
    }
    return ""
}

func (r *Redis) SAdd(key string, vals ...interface{}) int {
    slots := make([]string, 0, len(vals))
    for i := 0; i < len(vals); i++ {
        slots = append(slots,  "%v")
    }
    cmd := fmt.Sprintf("SADD %s %s", key, strings.Join(slots, " "))
    rets, err := r.Exec(fmt.Sprintf(cmd, vals...))
    if err == nil && len(rets) > 0 {
        num, _ := strconv.ParseInt(rets[0], 10, 64)
        return int(num)
    }
    return 0
}

func (r *Redis) SMembers(key string) []string {
    cmd := "SMEMBERS " + key
    rets, _ := r.Exec(cmd)
    return rets
}

func Test() {
    redis, _ := New("127.0.0.1", 6379, 0)

    redis.Set("key3", 34323523)
    ret := redis.String("key3")
    log.Print(ret)

    num := redis.Int("key3")
    log.Print(num)

    redis.LPush("list5", 4, 5, 6, 6, "aaa")

    n := redis.SAdd("set_0", 1, 10, 33, "dd", "d3", 4, 5, 5)
    rets := redis.SMembers("set_0")
    log.Print(n, rets)

    rets = redis.LRange("list", 0, -1)
    log.Print(rets)

    redis.SetDB(1)
    v := redis.BLPop("list", 0)
    log.Print(v, "--------")
}
