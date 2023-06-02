package writer

import "github.com/zhanghaiyang9999/RedisShake/internal/entry"

type Writer interface {
	Write(entry *entry.Entry)
	Close()
}
