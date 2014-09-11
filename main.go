package main


import (
    "github.com/roydong/gos/lib/redis"
)


func main() {
    redis.Test()
    select {}
}
