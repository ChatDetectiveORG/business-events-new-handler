package redis

import (
	"sync"

	"app/src/infrastructure/config"

	e "github.com/ChatDetectiveORG/shared/errors"

	"github.com/gomodule/redigo/redis"
)

var (
	poolOnce sync.Once
	pool     *redis.Pool
)

// GetPool returns a singleton redigo pool initialized from env.
func GetPool(appcfg *config.Config) (*redis.Pool, *e.ErrorInfo) {
	var initErr *e.ErrorInfo = e.Nil()

	poolOnce.Do(func() {
		pool = newPool(appcfg)
	})

	if !initErr.IsNil() {
		return nil, initErr
	}
	return pool, e.Nil()
}

// InitRedis opens a connection and ensures required Redis models.
//
// Notes about "models" in Redis:
//   - Redis doesn't have real schemas; this is an idempotent boot-time contract check.
//   - Some data structures cannot exist empty (set/hash/list/zset disappear when empty),
//     so templates seed a placeholder unless you customize Ensure() logic.
func InitRedis(appcfg *config.Config) *e.ErrorInfo {
	p, err := GetPool(appcfg)
	if !err.IsNil() {
		return err
	}

	conn := p.Get()
	defer func() { _ = conn.Close() }()

	if conn.Err() != nil {
		return e.FromError(conn.Err(), "failed to get redis connection").WithSeverity(e.Critical)
	}

	if _, pingErr := redis.String(conn.Do("PING")); pingErr != nil {
		return e.FromError(pingErr, "redis ping failed").WithSeverity(e.Critical)
	}

	return e.Nil()
}
