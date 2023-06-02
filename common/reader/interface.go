package reader

import "github.com/zhanghaiyang9999/RedisShake/common/entry"

type Reader interface {
	StartRead() chan *entry.Entry
}
