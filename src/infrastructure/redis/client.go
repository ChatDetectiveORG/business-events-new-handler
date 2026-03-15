package redis

import (
	"time"

	"app/src/infrastructure/config"

	"github.com/gomodule/redigo/redis"

	e "github.com/ChatDetectiveORG/shared/errors"
)

func newPool(config *config.Config) *redis.Pool {
	dial := func() (redis.Conn, error) {
		opts := []redis.DialOption{
			redis.DialConnectTimeout(config.RedisConfig.ConnectionTimeout),
			redis.DialReadTimeout(config.RedisConfig.ReadTimeout),
			redis.DialWriteTimeout(config.RedisConfig.WriteTimeout),
		}

		if config.RedisConfig.Password != "" {
			opts = append(opts, redis.DialPassword(config.RedisConfig.Password))
		}
		opts = append(opts, redis.DialDatabase(config.RedisConfig.Database))

		return redis.Dial("tcp", config.RedisConfig.Host+":"+config.RedisConfig.Port, opts...)
	}

	return &redis.Pool{
		// MaxIdle: максимальное количество простаивающих (неиспользуемых) соединений в пуле.
		MaxIdle: config.RedisConfig.MaxIdle,
		// MaxActive: максимальное количество одновременно открытых (активных) соединений к Redis.
		MaxActive: config.RedisConfig.MaxActive,
		// IdleTimeout: сколько времени соединение может "простаивать" в пуле, прежде чем будет закрыто.
		IdleTimeout: config.RedisConfig.IdleTimeout,
		// Wait: если значение true — новые запросы ожидают появления свободного соединения, когда пул переполнен (MaxActive), если false — сразу будет ошибка.
		Wait: config.RedisConfig.Wait,
		// Dial: функция, создающая новое соединение с Redis.
		Dial: dial,
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			// Avoid pinging too frequently.
			if time.Since(t) < 30*time.Second {
				return nil
			}
			_, err := redis.String(c.Do("PING"))
			return err
		},
	}
}

func NewRedisConnection(cfg *config.Config) (redis.Conn, *e.ErrorInfo) {
	pool, err := GetPool(cfg)
	if !err.IsNil() {
		return nil, err
	}

	return pool.Get(), e.Nil()
}
