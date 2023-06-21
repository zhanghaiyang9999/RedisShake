package reader

import "github.com/zhanghaiyang9999/RedisShake/common/entry"
import "github.com/zhanghaiyang9999/RedisShake/common/rdb"

type Reader interface {
	StartRead(notifier rdb.ReadNotifier) chan *entry.Entry
	SetWorkFolder(path string) error
}
