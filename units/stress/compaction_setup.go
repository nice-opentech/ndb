package main

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"log"
	"time"
	"os"
	"strconv"
)

func createNamespace(addr string, nsname string) {
	conn, err := redis.Dial("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	_, err = conn.Do("NSDEL", nsname)
	if err != nil {
		log.Fatal(err)
	}
	_, err = conn.Do("NSNEW", nsname)
	if err != nil {
		log.Fatal(err)
	}
}

func compact(addr string, nsname string) {
	conn, err := redis.Dial("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	_, err = conn.Do("COMPACT", nsname)
	if err != nil {
		log.Fatal(err)
	}
}

func setup(addr string, routines, count int, cb func(redis.Conn, int)) {
	done := make(chan bool)

	for r := 0; r < routines; r++ {
		go func(r int) {
			conn, err := redis.Dial("tcp", addr)
			if err != nil {
				log.Fatal(err)
			}
			defer conn.Close()
			for c := 0; c < count; c++ {
				cb(conn, r * count + c)
			}
			done <- true
		}(r)
	}
	for r := 0; r < routines; r++ {
		<-done
	}
}

type Callback func(conn redis.Conn, key string)

func setupExpire(addr string, nsname string, routines, count, expire int, cb Callback) {
	createNamespace(addr, nsname)

	// Expire 1, 2, 4, 8
	for i := 1; i <= 8; i *= 2 {
		setup(addr, routines, count, func(conn redis.Conn, id int) {
			key := fmt.Sprintf("%s:%08d", nsname, id)
			cb(conn, key)
			if id % i == 0 {
				_, err := conn.Do("EXPIRE", key, expire)
				if err != nil {
					log.Fatal(err)
				}
			}
		})
		fmt.Printf("Setup %s %d OK\n", nsname, i)
		time.Sleep(time.Duration(expire) * time.Second)
	}

	// Delete 3
	setup(addr, routines, count, func(conn redis.Conn, id int) {
		key := fmt.Sprintf("%s:%08d", nsname, id)
		if id % 3 == 0 {
			_, err := conn.Do("DEL", key)
			if err != nil {
				log.Fatal(err)
			}
		}
	})
	time.Sleep(time.Duration(expire) * time.Second)

	compact(addr, nsname)
	compact(addr, nsname)
	fmt.Printf("Setup %s OK\n", nsname)
}

func setupStr(conn redis.Conn, key string) {
	_, err := conn.Do("SET", key, key)
	if err != nil {
		log.Fatal(err)
	}
}

func setupInt(conn redis.Conn, key string) {
	_, err := conn.Do("INCR", key)
	if err != nil {
		log.Fatal(err)
	}
}

func setupSet(conn redis.Conn, key string) {
	_, err := conn.Do("SADD", key, "a", "b", "c")
	if err != nil {
		log.Fatal(err)
	}
}

func setupHash(conn redis.Conn, key string) {
	_, err := conn.Do("HMSET", key, "a", 1, "b", 2, "c", 3)
	if err != nil {
		log.Fatal(err)
	}
}

func setupList(conn redis.Conn, key string) {
	_, err := conn.Do("RPUSH", key, "a", "b", "c")
	if err != nil {
		log.Fatal(err)
	}
	_, err = conn.Do("LTRIM", key, 0, 2)
	if err != nil {
		log.Fatal(err)
	}
}

func setupZSet(conn redis.Conn, key string) {
	_, err := conn.Do("ZADD", key, 1, "a", 2, "b", 3, "c")
	if err != nil {
		log.Fatal(err)
	}
}

func setupXSet(conn redis.Conn, key string) {
	_, err := conn.Do("XADD", key, 1, "a", 2, "b", 3, "c")
	if err != nil {
		log.Fatal(err)
	}
}

func setupOSet(conn redis.Conn, key string) {
	_, err := conn.Do("OADD", key, 1, 2, 3)
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	if len(os.Args) != 3 {
		fmt.Println("Usage: compaction_setup <addr> <count>\n")
		os.Exit(1)
	}

	addr := os.Args[1]
	routines := 100
	count, _ := strconv.Atoi(os.Args[2])
	count /= routines
	expire := 1

	tests := map[string]Callback {
		"str": setupStr,
		"int": setupInt,
		"set": setupSet,
		"hash": setupHash,
		"list": setupList,
		"zset": setupZSet,
		"xset": setupXSet,
		"oset": setupOSet,
	}

	done := make(chan bool)
	for nsname, cb := range tests {
		go func(nsname string, cb Callback) {
			setupExpire(addr, nsname, routines, count, expire, cb)
			done <- true
		}(nsname, cb)
	}
	for i := 0; i < len(tests); i++ {
		<-done
	}
}
