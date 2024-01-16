package redis_client

import (
	"os"
	"strings"

	"github.com/go-redis/redis/v9"
)

func getRedisAddr() []string {
	raw_addr := os.Getenv("REDIS_ADDR")
	return strings.Split(raw_addr, ",")
}

func GetRedisClients() []*redis.Client {
	addr_arr := getRedisAddr()
	rdb_arr := make([]*redis.Client, len(addr_arr))
	for i := 0; i < len(addr_arr); i++ {
		rdb_arr[i] = redis.NewClient(&redis.Options{
			Addr:     addr_arr[i],
			Password: "", // no password set
			DB:       0,  // use default DB
		})
	}
	return rdb_arr
}
