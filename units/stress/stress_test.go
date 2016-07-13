package stress

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"github.com/stretchr/testify/assert"
	"log"
	"sort"
	"strconv"
	"testing"
)

func createNamespace(t *testing.T, addr string, nsname string) {
	conn, err := redis.Dial("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	{
		cmd := "NSDEL"
		_, err := conn.Do(cmd, nsname)
		assert.Equal(t, nil, err, cmd)
	}

	{
		cmd := "NSNEW"
		_, err := conn.Do(cmd, nsname)
		assert.Equal(t, nil, err, cmd)
	}
}

func testLock(c chan bool, t *testing.T, addr string, count int, length int, maxlen int) {
	done := make(chan bool)

	assert := assert.New(t)
	card := count * length

	for i := 0; i < count; i++ {
		go func(i int) {
			conn, err := redis.Dial("tcp", addr)
			if err != nil {
				log.Fatal(err)
			}
			defer conn.Close()

			// INCR
			for j := 0; j < length; j++ {
				cmd := "INCR"
				_, err := conn.Do(cmd, "int")
				assert.Equal(nil, err, cmd)
			}

			// RPUSH
			for j := 0; j < length; j++ {
				cmd := "RPUSH"
				member := fmt.Sprintf("%d", i*length+j)
				_, err := conn.Do(cmd, "list", member)
				assert.Equal(nil, err, cmd)
			}

			// HMSET
			for j := 0; j < length; j++ {
				cmd := "HMSET"
				member := fmt.Sprintf("%d", i*length+j)
				_, err := conn.Do(cmd, "hash", member, member, member, member, member, member)
				assert.Equal(nil, err, cmd)
			}

			// SADD
			for j := 0; j < length; j++ {
				cmd := "SADD"
				member := fmt.Sprintf("%d", i*length+j)
				n, err := conn.Do(cmd, "set", member, member, member)
				assert.Equal(nil, err, cmd)
				assert.Equal(int64(1), n.(int64), cmd)
			}

			// ZADD
			for j := 0; j < length; j++ {
				cmd := "ZADD"
				member := fmt.Sprintf("%d", i*length+j)
				n, err := conn.Do(cmd, "zset", member, member, member, member, member, member)
				assert.Equal(nil, err, cmd)
				assert.Equal(int64(1), n.(int64), cmd)
			}

			// XADD
			for j := 0; j < length; j++ {
				cmd := "XADD"
				member := fmt.Sprintf("%d", i*length+j)
				n, err := conn.Do(cmd, "xset", "maxlen", fmt.Sprintf("%d", card),
					member, member, member, member, member, member)
				assert.Equal(nil, err, cmd)
				assert.Equal(int64(1), n.(int64), cmd)
			}

			// OADD
			for j := 0; j < length; j++ {
				cmd := "OADD"
				member := fmt.Sprintf("%d", i*length+j)
				n, err := conn.Do(cmd, "oset", "maxlen", fmt.Sprintf("%d", card),
					member, member, member)
				assert.Equal(nil, err, cmd)
				assert.Equal(int64(1), n.(int64), cmd)
			}

			done <- true
		}(i)
	}

	for i := 0; i < count; i++ {
		<-done
	}

	conn, err := redis.Dial("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	// Check INCR
	{
		cmd := "GET"
		n, err := conn.Do(cmd, "int")
		assert.Equal(nil, err, cmd)
		assert.Equal([]byte(fmt.Sprintf("%d", card)), n.([]byte), cmd)
		checkDelete(assert, conn, []byte("int"))
	}

	// Check RPUSH
	{
		cmd := "LRANGE"
		a, err := conn.Do(cmd, "list", "0", "-1")
		assert.Equal(nil, err, cmd)
		checkListRange(t, cmd, a, card)
		checkDelete(assert, conn, []byte("list"))
	}

	// Check HMSET
	{
		cmd := "HGETALL"
		a, err := conn.Do(cmd, "hash")
		assert.Equal(nil, err, cmd)
		assert.Equal(2*card, len(a.([]interface{})), cmd)
		// checkHashRange(t, cmd, a, card)
		checkDelete(assert, conn, []byte("hash"))
	}

	// Check SADD
	{
		cmd := "SMEMBERS"
		a, err := conn.Do(cmd, "set")
		assert.Equal(nil, err, cmd)
		checkSetRange(t, cmd, a, card)
		checkDelete(assert, conn, []byte("set"))
	}

	// Check ZADD
	{
		cmd := "ZRANGE"
		a, err := conn.Do(cmd, "zset", "0", "-1", "WITHSCORES")
		assert.Equal(nil, err, cmd)
		checkZSetRange(t, cmd, a, card, false)
		checkDelete(assert, conn, []byte("zset"))
	}

	// Check XADD
	begin, xcard := 0, 0
	{
		cmd := "XCARD"
		n, err := conn.Do(cmd, "xset")
		assert.Equal(nil, err, cmd)
		xcard = int(n.(int64))
		begin = card - xcard
	}
	{
		cmd := "XRANGE"
		a, err := conn.Do(cmd, "xset", "0", "-1", "WITHSCORES")
		assert.Equal(nil, err, cmd)
		checkXSetRange(t, cmd, a, begin, xcard, false)
		checkDelete(assert, conn, []byte("xset"))
	}

	// Check OADD
	begin, ocard := 0, 0
	{
		cmd := "OCARD"
		n, err := conn.Do(cmd, "oset")
		assert.Equal(nil, err, cmd)
		ocard = int(n.(int64))
		begin = card - ocard
	}
	{
		cmd := "ORANGE"
		a, err := conn.Do(cmd, "oset", "0", "-1")
		assert.Equal(nil, err, cmd)
		checkOSetRange(t, cmd, a, begin, ocard, false)
		checkDelete(assert, conn, []byte("oset"))
	}

	c <- true
}

func testSet(c chan bool, t *testing.T, addr string, count int, length int, maxlen int) {
	done := make(chan bool)
	createNamespace(t, addr, "set");

	for i := 0; i < count; i++ {
		go func(i int) {
			conn, err := redis.Dial("tcp", addr)
			if err != nil {
				log.Fatal(err)
			}
			defer conn.Close()

			assert := assert.New(t)
			set := []byte(fmt.Sprintf("set:%d", i))

			for j := 0; j < length; j++ {
				scard := j + 1
				member := []byte(fmt.Sprintf("%d", j))

				// SADD
				{
					cmd := "SADD"
					_, err := conn.Do(cmd, set, member, member, member)
					assert.Equal(nil, err, cmd)
				}

				// SCARD
				{
					cmd := "SCARD"
					n, err := conn.Do(cmd, set)
					assert.Equal(nil, err, cmd)
					assert.Equal(int64(scard), n.(int64), cmd)
				}

				// SMEMBERS
				{
					cmd := "SMEMBERS"
					a, err := conn.Do(cmd, set)
					assert.Equal(nil, err, cmd)
					checkSetRange(t, cmd, a, scard)
				}
			}

			scard := length

			// SREM
			{
				cmd := "SREM"
				for i := scard / 2; i < scard; i++ {
					n, err := conn.Do(cmd, set, fmt.Sprintf("%d", i))
					assert.Equal(nil, err, cmd)
					assert.Equal(int64(1), n.(int64), cmd)
				}
			}

			scard /= 2

			// SMEMBERS
			{
				cmd := "SMEMBERS"
				a, err := conn.Do(cmd, set)
				assert.Equal(nil, err, cmd)
				checkSetRange(t, cmd, a, scard)
			}

			checkDelete(assert, conn, set)
			done <- true
		}(i)
	}

	for i := 0; i < count; i++ {
		<-done
	}

	c <- true
}

type Digits []interface{}

func (d Digits) Len() int      { return len(d) }
func (d Digits) Swap(i, j int) { d[i], d[j] = d[j], d[i] }
func (d Digits) Less(i, j int) bool {
	a, _ := strconv.Atoi(string(d[i].([]byte)))
	b, _ := strconv.Atoi(string(d[j].([]byte)))
	return a < b
}

func checkSetRange(t *testing.T, cmd string, a interface{}, length int) {
	assert := assert.New(t)
	items := a.([]interface{})
	sort.Sort(Digits(items))
	assert.Equal(len(items), length, cmd)
	for i := 0; i < length; i++ {
		assert.Equal(
			[]byte(fmt.Sprintf("%d", i)),
			items[i].([]byte),
			cmd)
	}
}

func testList(c chan bool, t *testing.T, addr string, count int, length int, maxlen int) {
	done := make(chan bool)
	createNamespace(t, addr, "list")

	for i := 0; i < count; i++ {
		go func(i int) {
			conn, err := redis.Dial("tcp", addr)
			if err != nil {
				log.Fatal(err)
			}
			defer conn.Close()

			assert := assert.New(t)
			list := []byte(fmt.Sprintf("list:%d", i))

			for j := 0; j < length; j++ {
				llen := j + 1
				member := []byte(fmt.Sprintf("%d", j))

				// RPUSH
				{
					cmd := "RPUSH"
					_, err := conn.Do(cmd, list, member)
					assert.Equal(nil, err, cmd)
				}

				// LLEN
				{
					cmd := "LLEN"
					n, err := conn.Do(cmd, list)
					assert.Equal(nil, err, cmd)
					assert.Equal(int64(llen), n.(int64), cmd)
				}

				// LINDEX
				{
					cmd := "LINDEX"
					s, err := conn.Do(cmd, list, "-1")
					assert.Equal(nil, err, cmd)
					assert.Equal(member, s.([]byte), cmd)
				}

				// LRANGE
				{
					cmd := "LRANGE"
					a, err := conn.Do(cmd, list, "0", "-1")
					assert.Equal(nil, err, cmd)
					checkListRange(t, cmd, a, llen)
				}
			}

			llen := length

			// LTRIM
			{
				cmd := "LTRIM"
				_, err := conn.Do(cmd, list, "0", fmt.Sprintf("%d", llen/2-1))
				assert.Equal(nil, err, cmd)
			}

			llen /= 2

			// LREM
			{
				cmd := "LREM"
				for i := llen / 2; i < llen; i++ {
					n, err := conn.Do(cmd, list, "0", fmt.Sprintf("%d", i))
					assert.Equal(nil, err, cmd)
					assert.Equal(int64(1), n.(int64), cmd)
				}
			}

			// LRANGE
			{
				cmd := "LRANGE"
				a, err := conn.Do(cmd, list, "0", "-1")
				assert.Equal(nil, err, cmd)
				checkListRange(t, cmd, a, llen/2)
			}

			checkDelete(assert, conn, list)
			done <- true
		}(i)
	}

	for i := 0; i < count; i++ {
		<-done
	}

	c <- true
}

func checkListRange(t *testing.T, cmd string, a interface{}, length int) {
	assert := assert.New(t)
	items := a.([]interface{})
	sort.Sort(Digits(items))
	assert.Equal(length, len(items), cmd)
	for i := 0; i < length; i++ {
		assert.Equal(
			[]byte(fmt.Sprintf("%d", i)),
			items[i].([]byte),
			cmd)
	}
}

func testHash(c chan bool, t *testing.T, addr string, count int, length int, maxlen int) {
	done := make(chan bool)
	createNamespace(t, addr, "hash")

	for i := 0; i < count; i++ {
		go func(i int) {
			conn, err := redis.Dial("tcp", addr)
			if err != nil {
				log.Fatal(err)
			}
			defer conn.Close()

			assert := assert.New(t)
			hash := []byte(fmt.Sprintf("hash:%d", i))

			for j := 0; j < length; j++ {
				hlen := j + 1
				field := []byte(fmt.Sprintf("%d", j))
				value := []byte(fmt.Sprintf("%d", j))

				// HSET
				{
					cmd := "HSET"
					_, err := conn.Do(cmd, hash, field, value)
					assert.Equal(nil, err, cmd)
				}

				// HGET
				{
					cmd := "HGET"
					s, err := conn.Do(cmd, hash, field)
					assert.Equal(nil, err, cmd)
					assert.Equal(value, s.([]byte), cmd)
				}

				// HMSET
				{
					cmd := "HMSET"
					_, err := conn.Do(cmd, hash, field, value, field, value, field, value)
					assert.Equal(nil, err, cmd)
				}
				{
					cmd := "HMGET"
					ss, err := conn.Do(cmd, hash, field, field)
					assert.Equal(nil, err, cmd)
					items := ss.([]interface{})
					assert.Equal(2, len(items), cmd)
					assert.Equal(value, items[0].([]byte), cmd)
					assert.Equal(value, items[1].([]byte), cmd)
				}

				// HEXISTS
				{
					cmd := "HEXISTS"
					n, err := conn.Do(cmd, hash, field)
					assert.Equal(nil, err, cmd)
					assert.Equal(int64(1), n.(int64), cmd)
				}

				// HLEN
				{
					cmd := "HLEN"
					n, err := conn.Do(cmd, hash)
					assert.Equal(nil, err, cmd)
					assert.Equal(int64(hlen), n.(int64), cmd)
				}

				// HGETALL key
				{
					cmd := "HGETALL"
					a, err := conn.Do(cmd, hash)
					assert.Equal(nil, err, cmd)
					checkHashRange(t, cmd, a, hlen)
				}
			}

			hlen := length

			// HDEL
			{
				cmd := "HDEL"
				for i := hlen / 2; i < hlen; i++ {
					n, err := conn.Do(cmd, hash, fmt.Sprintf("%d", i))
					assert.Equal(nil, err, cmd)
					assert.Equal(int64(1), n.(int64), cmd)
				}
			}

			hlen /= 2

			// HGETALL
			{
				cmd := "HGETALL"
				a, err := conn.Do(cmd, hash)
				assert.Equal(nil, err, cmd)
				checkHashRange(t, cmd, a, hlen)
			}

			checkDelete(assert, conn, hash)
			done <- true
		}(i)
	}

	for i := 0; i < count; i++ {
		<-done
	}

	c <- true
}

func checkHashRange(t *testing.T, cmd string, a interface{}, length int) {
	assert := assert.New(t)
	items := a.([]interface{})
	sort.Sort(Digits(items))
	assert.Equal(2*length, len(items), cmd)
	for i := 0; i < length; i++ {
		assert.Equal(
			[]byte(fmt.Sprintf("%d", i)),
			items[2*i].([]byte),
			cmd)
		assert.Equal(
			[]byte(fmt.Sprintf("%d", i)),
			items[2*i+1].([]byte),
			cmd)
	}
}

func testZSet(c chan bool, t *testing.T, addr string, count int, length int, maxlen int) {
	done := make(chan bool)
	createNamespace(t, addr, "zset")

	for i := 0; i < count; i++ {
		go func(i int) {
			conn, err := redis.Dial("tcp", addr)
			if err != nil {
				log.Fatal(err)
			}
			defer conn.Close()

			assert := assert.New(t)
			zset := []byte(fmt.Sprintf("zset:%d", i))

			for j := 0; j < length; j++ {
				zcard := j + 1
				score := []byte(fmt.Sprintf("%d", j))
				member := []byte(fmt.Sprintf("%d", j))

				// ZADD
				{
					cmd := "ZADD"
					_, err := conn.Do(cmd, zset, score, member, score, member, score, member)
					assert.Equal(nil, err, cmd)
				}

				// ZCARD
				{
					cmd := "ZCARD"
					n, err := conn.Do(cmd, zset)
					assert.Equal(nil, err, cmd)
					assert.Equal(int64(zcard), n.(int64), cmd)
				}

				// ZSCORE
				{
					cmd := "ZSCORE"
					s, err := conn.Do(cmd, zset, member)
					assert.Equal(nil, err, cmd)
					assert.Equal(score, s.([]byte), cmd)
				}

				// ZRANGE
				{
					cmd := "ZRANGE"
					a, err := conn.Do(cmd, zset, "0", "-1", "WITHSCORES")
					assert.Equal(nil, err, cmd)
					checkZSetRange(t, cmd, a, zcard, false)
				}

				// ZREVRANGE
				{
					cmd := "ZREVRANGE"
					a, err := conn.Do(cmd, zset, "0", "-1", "WITHSCORES")
					assert.Equal(nil, err, cmd)
					checkZSetRange(t, cmd, a, zcard, true)
				}

				// ZRANGEBYSCORE
				{
					cmd := "ZRANGEBYSCORE"
					a, err := conn.Do(cmd, zset, "-inf", "+inf", "WITHSCORES")
					assert.Equal(nil, err, cmd)
					checkZSetRange(t, cmd, a, zcard, false)
				}

				// ZREVRANGEBYSCORE
				{
					cmd := "ZREVRANGEBYSCORE"
					a, err := conn.Do(cmd, zset, "+inf", "-inf", "WITHSCORES")
					assert.Equal(nil, err, cmd)
					checkZSetRange(t, cmd, a, zcard, true)
				}
			}

			zcard := length

			// ZREM
			{
				cmd := "ZREM"
				for i := zcard / 2; i < zcard; i++ {
					n, err := conn.Do(cmd, zset, fmt.Sprintf("%d", i))
					assert.Equal(nil, err, cmd)
					assert.Equal(int64(1), n.(int64), cmd)
				}
			}
			{
				cmd := "ZRANGE"
				a, err := conn.Do(cmd, zset, "0", "-1", "WITHSCORES")
				assert.Equal(nil, err, cmd)
				checkZSetRange(t, cmd, a, zcard/2, false)
			}

			zcard /= 2

			// ZREMRANGEBYRANK
			{
				cmd := "ZREMRANGEBYRANK"
				n, err := conn.Do(cmd, zset, fmt.Sprintf("%d", zcard/2), "-1")
				assert.Equal(nil, err, cmd)
				assert.Equal(int64(zcard/2), n.(int64), cmd)
			}
			{
				cmd := "ZREVRANGE"
				a, err := conn.Do(cmd, zset, "0", "-1", "WITHSCORES")
				assert.Equal(nil, err, cmd)
				checkZSetRange(t, cmd, a, zcard/2, true)
			}

			checkDelete(assert, conn, zset)
			done <- true
		}(i)
	}

	for i := 0; i < count; i++ {
		<-done
	}

	c <- true
}

func checkZSetRange(t *testing.T, cmd string, a interface{}, length int, reverse bool) {
	checkXSetRange(t, cmd, a, 0, length, reverse)
}

func testXSet(c chan bool, t *testing.T, addr string, count int, length int, maxlen int) {
	done := make(chan bool)
	createNamespace(t, addr, "xset")

	for i := 0; i < count; i++ {
		go func(i int) {
			conn, err := redis.Dial("tcp", addr)
			if err != nil {
				log.Fatal(err)
			}
			defer conn.Close()

			assert := assert.New(t)
			xset := []byte(fmt.Sprintf("xset:%d", i))

			for j := 0; j < length; j++ {
				begin, xcard := 0, j+1
				if xcard > maxlen {
					begin, xcard = xcard-maxlen, maxlen
				}
				score := []byte(fmt.Sprintf("%d", j))
				member := []byte(fmt.Sprintf("%d", j))

				// XADD
				{
					cmd := "XADD"
					_, err := conn.Do(cmd, xset, "MAXLEN", fmt.Sprintf("%d", maxlen),
						score, member, score, member, score, member)
					assert.Equal(nil, err, cmd)
				}

				// XCARD
				{
					cmd := "XCARD"
					n, err := conn.Do(cmd, xset)
					assert.Equal(nil, err, cmd)
					if xcard == j+1 {
						assert.Equal(int64(xcard), n.(int64), cmd)
					} else {
						assert.Condition(func() bool {
							return int64(xcard) <= n.(int64) && 2*int64(xcard) > n.(int64)
						}, cmd)
					}
					xcard = int(n.(int64))
					begin = j + 1 - xcard
				}

				// XSCORE
				{
					cmd := "XSCORE"
					s, err := conn.Do(cmd, xset, member)
					assert.Equal(nil, err, cmd)
					assert.Equal(score, s.([]byte), cmd)
				}

				// XRANGE
				{
					cmd := "XRANGE"
					a, err := conn.Do(cmd, xset, "0", "-1", "WITHSCORES")
					assert.Equal(nil, err, cmd)
					checkXSetRange(t, cmd, a, begin, xcard, false)
				}

				// XREVRANGE
				{
					cmd := "XREVRANGE"
					a, err := conn.Do(cmd, xset, "0", "-1", "WITHSCORES")
					assert.Equal(nil, err, cmd)
					checkXSetRange(t, cmd, a, begin, xcard, true)
				}

				// XRANGEBYSCORE
				{
					cmd := "XRANGEBYSCORE"
					a, err := conn.Do(cmd, xset, "-inf", "+inf", "WITHSCORES")
					assert.Equal(nil, err, cmd)
					checkXSetRange(t, cmd, a, begin, xcard, false)
				}

				// XREVRANGEBYSCORE
				{
					cmd := "XREVRANGEBYSCORE"
					a, err := conn.Do(cmd, xset, "+inf", "-inf", "WITHSCORES")
					assert.Equal(nil, err, cmd)
					checkXSetRange(t, cmd, a, begin, xcard, true)
				}
			}

			begin, xcard := 0, length

			// XCARD
			{
				cmd := "XCARD"
				n, err := conn.Do(cmd, xset)
				assert.Equal(nil, err, cmd)
				xcard = int(n.(int64))
				begin = length - xcard
			}

			// XGETMAXLEN
			{
				cmd := "XGETMAXLEN"
				n, err := conn.Do(cmd, xset)
				assert.Equal(nil, err, cmd)
				assert.Equal(int64(maxlen), n.(int64), cmd)
			}

			// XGETFINITY
			{
				cmd := "XGETFINITY"
				n, err := conn.Do(cmd, xset)
				assert.Equal(nil, err, cmd)
				assert.Equal(int64(maxlen), n.(int64), cmd)
			}

			// XREM
			{
				cmd := "XREM"
				for i := xcard / 2; i < xcard; i++ {
					n, err := conn.Do(cmd, xset, fmt.Sprintf("%d", i+begin))
					assert.Equal(nil, err, cmd)
					assert.Equal(int64(1), n.(int64), cmd)
				}
			}

			{
				cmd := "XRANGE"
				a, err := conn.Do(cmd, xset, "0", "-1", "WITHSCORES")
				assert.Equal(nil, err, cmd)
				checkXSetRange(t, cmd, a, begin, xcard/2, false)
			}

			xcard /= 2

			// XREMRANGEBYRANK
			{
				cmd := "XREMRANGEBYRANK"
				n, err := conn.Do(cmd, xset, fmt.Sprintf("%d", xcard/2), "-1")
				assert.Equal(nil, err, cmd)
				assert.Equal(int64(xcard-xcard/2), n.(int64), cmd)
			}
			{
				cmd := "XREVRANGE"
				a, err := conn.Do(cmd, xset, "0", "-1", "WITHSCORES")
				assert.Equal(nil, err, cmd)
				checkXSetRange(t, cmd, a, begin, xcard/2, true)
			}

			checkDelete(assert, conn, xset)
			done <- true
		}(i)
	}

	for i := 0; i < count; i++ {
		<-done
	}

	c <- true
}

func checkXSetRange(t *testing.T, cmd string, a interface{}, begin int, length int, reverse bool) {
	assert := assert.New(t)
	items := a.([]interface{})
	assert.Equal(2*length, len(items), cmd)
	if !reverse {
		for i := 0; i < length; i++ {
			assert.Equal(
				[]byte(fmt.Sprintf("%d", i+begin)),
				items[2*i].([]byte),
				cmd)
			assert.Equal(
				[]byte(fmt.Sprintf("%d", i+begin)),
				items[2*i+1].([]byte),
				cmd)
		}
	} else {
		for i := 0; i < length; i++ {
			assert.Equal(
				[]byte(fmt.Sprintf("%d", length-1-i+begin)),
				items[2*i].([]byte),
				cmd)
			assert.Equal(
				[]byte(fmt.Sprintf("%d", length-1-i+begin)),
				items[2*i+1].([]byte),
				cmd)
		}
	}
}

func testOSet(c chan bool, t *testing.T, addr string, count int, length int, maxlen int) {
	done := make(chan bool)
	createNamespace(t, addr, "oset")

	for i := 0; i < count; i++ {
		go func(i int) {
			conn, err := redis.Dial("tcp", addr)
			if err != nil {
				log.Fatal(err)
			}
			defer conn.Close()

			assert := assert.New(t)
			oset := []byte(fmt.Sprintf("oset:%d", i))

			for j := 0; j < length; j++ {
				begin, ocard := 0, j+1
				if ocard > maxlen {
					begin, ocard = ocard-maxlen, maxlen
				}
				member := []byte(fmt.Sprintf("%d", j))

				// OADD
				{
					cmd := "OADD"
					_, err := conn.Do(cmd, oset, "MAXLEN", fmt.Sprintf("%d", maxlen),
						member, member, member)
					assert.Equal(nil, err, cmd)
				}

				// OCARD
				{
					cmd := "OCARD"
					n, err := conn.Do(cmd, oset)
					assert.Equal(nil, err, cmd)
					if ocard == j+1 {
						assert.Equal(int64(ocard), n.(int64), cmd)
					} else {
						assert.Condition(func() bool {
							return int64(ocard) <= n.(int64) && 2*int64(ocard) > n.(int64)
						}, cmd)
					}
					ocard = int(n.(int64))
					begin = j + 1 - ocard
				}

				// ORANGE
				{
					cmd := "ORANGE"
					a, err := conn.Do(cmd, oset, "0", "-1")
					assert.Equal(nil, err, cmd)
					checkOSetRange(t, cmd, a, begin, ocard, false)
				}

				// OREVRANGE
				{
					cmd := "OREVRANGE"
					a, err := conn.Do(cmd, oset, "0", "-1")
					assert.Equal(nil, err, cmd)
					checkOSetRange(t, cmd, a, begin, ocard, true)
				}

				// ORANGEBYMEMBER
				{
					cmd := "ORANGEBYMEMBER"
					a, err := conn.Do(cmd, oset, "-inf", "+inf")
					assert.Equal(nil, err, cmd)
					checkOSetRange(t, cmd, a, begin, ocard, false)
				}

				// XREVRANGEBYMEMBER
				{
					cmd := "OREVRANGEBYMEMBER"
					a, err := conn.Do(cmd, oset, "+inf", "-inf")
					assert.Equal(nil, err, cmd)
					checkOSetRange(t, cmd, a, begin, ocard, true)
				}
			}

			begin, ocard := 0, length

			// OCARD
			{
				cmd := "OCARD"
				n, err := conn.Do(cmd, oset)
				assert.Equal(nil, err, cmd)
				ocard = int(n.(int64))
				begin = length - ocard
			}

			// OGETMAXLEN
			{
				cmd := "OGETMAXLEN"
				n, err := conn.Do(cmd, oset)
				assert.Equal(nil, err, cmd)
				assert.Equal(int64(maxlen), n.(int64), cmd)
			}

			// OGETFINITY
			{
				cmd := "OGETFINITY"
				n, err := conn.Do(cmd, oset)
				assert.Equal(nil, err, cmd)
				assert.Equal(int64(maxlen), n.(int64), cmd)
			}

			// OREM
			{
				cmd := "OREM"
				for i := ocard / 2; i < ocard; i++ {
					n, err := conn.Do(cmd, oset, fmt.Sprintf("%d", i+begin))
					assert.Equal(nil, err, cmd)
					assert.Equal(int64(1), n.(int64), cmd)
				}
			}

			{
				cmd := "ORANGE"
				a, err := conn.Do(cmd, oset, "0", "-1")
				assert.Equal(nil, err, cmd)
				checkOSetRange(t, cmd, a, begin, ocard/2, false)
			}

			ocard /= 2

			// OREMRANGEBYRANK
			{
				cmd := "OREMRANGEBYRANK"
				n, err := conn.Do(cmd, oset, fmt.Sprintf("%d", ocard/2), "-1")
				assert.Equal(nil, err, cmd)
				assert.Equal(int64(ocard-ocard/2), n.(int64), cmd)
			}
			{
				cmd := "OREVRANGE"
				a, err := conn.Do(cmd, oset, "0", "-1")
				assert.Equal(nil, err, cmd)
				checkOSetRange(t, cmd, a, begin, ocard/2, true)
			}

			checkDelete(assert, conn, oset)
			done <- true
		}(i)
	}

	for i := 0; i < count; i++ {
		<-done
	}

	c <- true
}

func checkOSetRange(t *testing.T, cmd string, a interface{}, begin int, length int, reverse bool) {
	assert := assert.New(t)
	items := a.([]interface{})
	assert.Equal(length, len(items), cmd)
	if !reverse {
		for i := 0; i < length; i++ {
			assert.Equal(int64(i+begin), items[i].(int64), cmd)
		}
	} else {
		for i := 0; i < length; i++ {
			assert.Equal(int64(length-1-i+begin), items[i].(int64), cmd)
		}
	}
}

func checkDelete(assert *assert.Assertions, conn redis.Conn, kmeta []byte) {
	// EXISTS
	{
		cmd := "EXISTS"
		n, err := conn.Do(cmd, kmeta)
		assert.Equal(err, nil, cmd)
		assert.Equal(int64(1), n.(int64), cmd)
	}

	// DEL
	{
		cmd := "DEL"
		n, err := conn.Do(cmd, kmeta)
		assert.Equal(err, nil, cmd)
		assert.Equal(int64(1), n.(int64), cmd)
	}

	// EXISTS
	{
		cmd := "EXISTS"
		n, err := conn.Do(cmd, kmeta)
		assert.Equal(err, nil, cmd)
		assert.Equal(int64(0), n.(int64), cmd)
	}
}

func Test(t *testing.T) {
	addr := ":9736"
	count, length, maxlen := 128, 128, 32

	done := make(chan bool)
	tests := []func(chan bool, *testing.T, string, int, int, int){
		testSet,
		testList,
		testHash,
		testZSet,
		testXSet,
		testOSet,
	}
	for _, test := range tests {
		go test(done, t, addr, count, length, maxlen)
	}
	for _ = range tests {
		<-done
	}
	go testLock(done, t, addr, count, length, length)
	<-done
}
