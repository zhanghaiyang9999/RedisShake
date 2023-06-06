package writer

import (
	"bytes"
	"github.com/zhanghaiyang9999/RedisShake/common/client"
	"github.com/zhanghaiyang9999/RedisShake/common/client/proto"
	"github.com/zhanghaiyang9999/RedisShake/common/config"
	"github.com/zhanghaiyang9999/RedisShake/common/entry"
	"github.com/zhanghaiyang9999/RedisShake/common/log"
	"github.com/zhanghaiyang9999/RedisShake/common/statistics"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type redisWriter struct {
	client *client.Redis
	DbId   int

	cmdBuffer   *bytes.Buffer
	chWaitReply chan *entry.Entry
	chWg        sync.WaitGroup

	UpdateUnansweredBytesCount uint64 // have sent in bytes
}

func NewRedisWriter(address string, username string, password string, isTls bool) (Writer, error) {
	var err error
	rw := new(redisWriter)
	rw.client, err = client.NewRedisClient(address, username, password, isTls)
	log.Infof("redisWriter connected to redis successful. address=[%s]", address)
	rw.cmdBuffer = new(bytes.Buffer)
	rw.chWaitReply = make(chan *entry.Entry, config.Config.Advanced.PipelineCountLimit)
	rw.chWg.Add(1)
	go rw.flushInterval()
	return rw, err
}

func (w *redisWriter) Write(e *entry.Entry) error {
	// switch db if we need
	if w.DbId != e.DbId {
		w.switchDbTo(e.DbId)
	}

	// send
	w.cmdBuffer.Reset()
	client.EncodeArgv(e.Argv, w.cmdBuffer)
	e.EncodedSize = uint64(w.cmdBuffer.Len())
	for e.EncodedSize+atomic.LoadUint64(&w.UpdateUnansweredBytesCount) > config.Config.Advanced.TargetRedisClientMaxQuerybufLen {
		time.Sleep(1 * time.Nanosecond)
	}
	w.chWaitReply <- e
	atomic.AddUint64(&w.UpdateUnansweredBytesCount, e.EncodedSize)
	return w.client.SendBytes(w.cmdBuffer.Bytes())
}

func (w *redisWriter) switchDbTo(newDbId int) error {
	err := w.client.Send("select", strconv.Itoa(newDbId))
	if err != nil {
		return err
	}
	w.DbId = newDbId
	w.chWaitReply <- &entry.Entry{
		Argv:    []string{"select", strconv.Itoa(newDbId)},
		CmdName: "select",
	}
	return nil
}

func (w *redisWriter) flushInterval() {
	for e := range w.chWaitReply {
		reply, err := w.client.Receive()
		if err == proto.Nil {
			log.Warnf("redisWriter receive nil reply. argv=%v", e.Argv)
		} else if err != nil {
			if err.Error() == "BUSYKEY Target key name already exists." {
				if config.Config.Advanced.RDBRestoreCommandBehavior == "skip" {
					log.Warnf("redisWriter received BUSYKEY reply. argv=%v", e.Argv)
				} else if config.Config.Advanced.RDBRestoreCommandBehavior == "panic" {
					log.Panicf("redisWriter received BUSYKEY reply. argv=%v", e.Argv)
				}
			} else {
				log.Warnf("redisWriter received error. error=[%v], argv=%v, slots=%v, reply=[%v]", err, e.Argv, e.Slots, reply)
			}
		}
		if strings.EqualFold(e.CmdName, "select") { // skip select command
			continue
		}
		atomic.AddUint64(&w.UpdateUnansweredBytesCount, ^(e.EncodedSize - 1))
		statistics.UpdateAOFAppliedOffset(uint64(e.Offset))
		statistics.UpdateUnansweredBytesCount(atomic.LoadUint64(&w.UpdateUnansweredBytesCount))
	}
	w.chWg.Done()
}

func (w *redisWriter) Close() {
	close(w.chWaitReply)
	w.chWg.Wait()
}
