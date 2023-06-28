package reader

import (
	"github.com/zhanghaiyang9999/RedisShake/common/entry"
	"github.com/zhanghaiyang9999/RedisShake/common/log"
	"github.com/zhanghaiyang9999/RedisShake/common/rdb"
	"github.com/zhanghaiyang9999/RedisShake/common/statistics"
	"os"
	"path/filepath"
)

type rdbReader struct {
	path string
	ch   chan *entry.Entry
}

func NewRDBReader(path string) Reader {
	log.Infof("NewRDBReader: path=[%s]", path)
	absolutePath, err := filepath.Abs(path)
	if err != nil {
		log.Panicf("NewRDBReader: filepath.Abs error: %s", err.Error())
	}
	log.Infof("NewRDBReader: absolute path=[%s]", absolutePath)
	r := new(rdbReader)
	r.path = absolutePath
	return r
}
func (r *rdbReader) SetWorkFolder(path string) error {
	return nil
}
func (r *rdbReader) DoWithReply(args ...string) (interface{}, error) {
	return nil, nil
}
func (r *rdbReader) StartRead(notifier rdb.ReadNotifier) chan *entry.Entry {
	r.ch = make(chan *entry.Entry, 1024)

	go func() {
		// start parse rdb
		log.Infof("start send RDB. path=[%s]", r.path)
		fi, err := os.Stat(r.path)
		if err != nil {
			log.Panicf("NewRDBReader: os.Stat error: %s", err.Error())
		}
		statistics.Metrics.RdbFileSize = uint64(fi.Size())
		statistics.Metrics.RdbReceivedSize = uint64(fi.Size())
		rdbLoader := rdb.NewLoader(r.path, r.ch, notifier)
		_ = rdbLoader.ParseRDB()
		log.Infof("send RDB finished. path=[%s]", r.path)
		close(r.ch)
	}()

	return r.ch
}
