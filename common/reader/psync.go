package reader

import (
	"bufio"
	"errors"
	"github.com/zhanghaiyang9999/RedisShake/common/client"
	"github.com/zhanghaiyang9999/RedisShake/common/entry"
	"github.com/zhanghaiyang9999/RedisShake/common/log"
	"github.com/zhanghaiyang9999/RedisShake/common/rdb"
	"github.com/zhanghaiyang9999/RedisShake/common/reader/rotate"
	"github.com/zhanghaiyang9999/RedisShake/common/statistics"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"time"
)

type psyncReader struct {
	client  *client.Redis
	address string
	ch      chan *entry.Entry
	DbId    int

	rd               *bufio.Reader
	receivedOffset   int64
	elastiCachePSync string
	//rdb file name
	workFolder string
	notifier   rdb.ReadNotifier
}

func NewPSyncReader(address string, username string, password string, isTls bool, ElastiCachePSync string) (Reader, error) {
	var err error
	r := new(psyncReader)
	r.address = address
	r.elastiCachePSync = ElastiCachePSync
	r.client, err = client.NewRedisClient(address, username, password, isTls)
	if err != nil {
		return r, err
	}
	r.rd = r.client.BufioReader()
	log.Infof("psyncReader connected to redis successful. address=[%s]", address)
	return r, nil
}
func (r *psyncReader) SetWorkFolder(path string) error {
	r.workFolder = path
	//create the folder
	_, err := os.Stat(path)
	if err != nil {
		return os.MkdirAll(path, os.ModePerm)
	}
	return nil
}
func (r *psyncReader) StartRead(notifier rdb.ReadNotifier) chan *entry.Entry {
	r.ch = make(chan *entry.Entry, 1024)
	r.notifier = notifier
	go func() {
		r.clearDir()
		go r.sendReplconfAck()
		r.saveRDB()
		startOffset := r.receivedOffset
		go r.saveAOF(r.rd)
		r.sendRDB()
		time.Sleep(1 * time.Second) // wait for saveAOF create aof file
		r.sendAOF(startOffset)
	}()

	return r.ch
}

func (r *psyncReader) clearDir() {
	files, err := ioutil.ReadDir(r.workFolder)
	if err != nil {
		log.PanicError(err)
	}

	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".rdb") || strings.HasSuffix(f.Name(), ".aof") {
			err = os.Remove(r.workFolder + "/" + f.Name())
			if err != nil {
				log.PanicError(err)
			}
			log.Warnf("remove file. filename=[%s]", f.Name())
		}
	}
}

func (r *psyncReader) saveRDB() error {
	log.Infof("start save RDB. address=[%s]", r.address)
	argv := []string{"replconf", "listening-port", "10007"} // 10007 is magic number
	log.Infof("send %v", argv)
	reply, _ := r.client.DoWithStringReply(argv...)
	if reply != "OK" {
		log.Warnf("send replconf command to redis server failed. address=[%s], reply=[%s], error=[]", r.address, reply)
	}

	// send psync
	argv = []string{"PSYNC", "?", "-1"}
	if r.elastiCachePSync != "" {
		argv = []string{r.elastiCachePSync, "?", "-1"}
	}
	r.client.Send(argv...)
	log.Infof("send %v", argv)
	// format: \n\n\n$<reply>\r\n
	for true {
		if r.notifier.IsStopped() {
			break
		}
		// \n\n\n$
		b, err := r.rd.ReadByte()
		if err != nil {
			log.PanicError(err)
		}
		if b == '\n' {
			continue
		}
		if b == '-' {
			reply, err := r.rd.ReadString('\n')
			if err != nil {
				return err
			}
			reply = strings.TrimSpace(reply)
			return errors.New(reply)
		}
		if b != '+' {
			return errors.New("invalid rdb format:" + string(b))
		}
		break
	}
	reply, err := r.rd.ReadString('\n')
	if err != nil {
		return err
	}
	reply = strings.TrimSpace(reply)
	log.Infof("receive [%s]", reply)
	masterOffset, err := strconv.Atoi(strings.Split(reply, " ")[2])
	if err != nil {
		return err
	}
	r.receivedOffset = int64(masterOffset)

	log.Infof("source db is doing bgsave. address=[%s]", r.address)
	statistics.Metrics.IsDoingBgsave = true

	timeStart := time.Now()
	// format: \n\n\n$<length>\r\n<rdb>
	for true {
		if r.notifier.IsStopped() {
			break
		}
		// \n\n\n$
		b, err := r.rd.ReadByte()
		if err != nil {
			return err
		}
		if b == '\n' {
			continue
		}
		if b != '$' {
			return errors.New("invalid rdb format:" + string(b))
		}
		break
	}
	statistics.Metrics.IsDoingBgsave = false
	log.Infof("source db bgsave finished. timeUsed=[%.2f]s, address=[%s]", time.Since(timeStart).Seconds(), r.address)
	lengthStr, err := r.rd.ReadString('\n')
	if err != nil {
		return err
	}
	lengthStr = strings.TrimSpace(lengthStr)
	length, err := strconv.ParseInt(lengthStr, 10, 64)
	if err != nil {
		return err
	}
	log.Infof("received rdb length. length=[%d]", length)
	statistics.SetRDBFileSize(uint64(length))

	// create rdb file
	rdbFilePath := r.workFolder + "/dump.rdb"

	log.Infof("create dump.rdb file. filename_path=[%s]", rdbFilePath)
	rdbFileHandle, err := os.OpenFile(rdbFilePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}

	// read rdb
	remainder := length
	const bufSize int64 = 32 * 1024 * 1024 // 32MB
	buf := make([]byte, bufSize)
	for remainder != 0 {
		if r.notifier.IsStopped() {
			break
		}
		readOnce := bufSize
		if remainder < readOnce {
			readOnce = remainder
		}
		n, err := r.rd.Read(buf[:readOnce])
		if err != nil {
			rdbFileHandle.Close()
			return err
		}
		remainder -= int64(n)
		statistics.UpdateRDBReceivedSize(uint64(length - remainder))
		_, err = rdbFileHandle.Write(buf[:n])
		if err != nil {
			rdbFileHandle.Close()
			return err
		}
	}
	err = rdbFileHandle.Close()
	if err != nil {
		return err
	}
	log.Infof("save RDB finished. address=[%s], total_bytes=[%d]", r.address, length)
	return nil
}

func (r *psyncReader) saveAOF(rd io.Reader) error {
	log.Infof("start save AOF. address=[%s]", r.address)
	// create aof file
	aofWriter := rotate.NewAOFWriter(r.workFolder, r.receivedOffset)
	defer aofWriter.Close()
	buf := make([]byte, 16*1024) // 16KB is enough for writing file
	for {
		if r.notifier.IsStopped() {
			break
		}
		n, err := rd.Read(buf)
		if err != nil {
			return err
		}
		r.receivedOffset += int64(n)
		statistics.UpdateAOFReceivedOffset(uint64(r.receivedOffset))
		aofWriter.Write(buf[:n])
	}
	return nil
}

func (r *psyncReader) sendRDB() {
	// start parse rdb
	log.Infof("start send RDB. address=[%s]", r.address)
	rdbFilePath := r.workFolder + "/dump.rdb"
	rdbLoader := rdb.NewLoader(rdbFilePath, r.ch, r.notifier)
	r.DbId = rdbLoader.ParseRDB()
	//remove the rdb file
	os.Remove(rdbFilePath)
	if r.notifier != nil {
		r.notifier.Notify("sync", 100)
	}
	log.Infof("send RDB finished. address=[%s], repl-stream-db=[%d]", r.address, r.DbId)
}

func (r *psyncReader) sendAOF(offset int64) {
	if r.notifier.IsStopped() {
		return
	}
	aofReader := rotate.NewAOFReader(r.workFolder, offset)
	defer aofReader.Close()
	r.client.SetBufioReader(bufio.NewReader(aofReader))
	for {
		if r.notifier.IsStopped() {
			e := entry.NewEntry()
			r.ch <- e
			break
		}
		argv := client.ArrayString(r.client.Receive())
		// select
		if strings.EqualFold(argv[0], "select") {
			DbId, err := strconv.Atoi(argv[1])
			if err != nil {
				log.PanicError(err)
			}
			r.DbId = DbId
			continue
		}

		e := entry.NewEntry()
		e.Argv = argv
		e.DbId = r.DbId
		e.Offset = aofReader.Offset()
		r.ch <- e
	}
}

func (r *psyncReader) sendReplconfAck() {
	for range time.Tick(time.Millisecond * 100) {
		// send ack receivedOffset
		r.client.Send("replconf", "ack", strconv.FormatInt(r.receivedOffset, 10))
	}
}
