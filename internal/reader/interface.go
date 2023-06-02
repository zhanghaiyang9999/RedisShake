package reader

import "github.com/zhanghaiyang9999/RedisShake/internal/entry"

type Reader interface {
	StartRead() chan *entry.Entry
}
