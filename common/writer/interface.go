package writer

import "github.com/zhanghaiyang9999/RedisShake/common/entry"

type Writer interface {
	Write(entry *entry.Entry)error
	Close()
	DoWithReply(args ...string) (interface{}, error)
}
