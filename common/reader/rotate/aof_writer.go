package rotate

import (
	"fmt"
	"github.com/zhanghaiyang9999/RedisShake/common/log"
	"os"
)

const MaxFileSize = 1024 * 1024 * 1024 // 1G

type AOFWriter struct {
	file     *os.File
	offset   int64
	folder   string
	filename string
	filesize int64
}

func NewAOFWriter(folder string, offset int64) *AOFWriter {
	w := &AOFWriter{}
	w.folder = folder
	w.openFile(offset)
	return w
}

func (w *AOFWriter) openFile(offset int64) {
	w.filename = fmt.Sprintf("%s/%d.aof", w.folder, offset)
	var err error
	w.file, err = os.OpenFile(w.filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		log.PanicError(err)
	}
	w.offset = offset
	w.filesize = 0
	log.Infof("AOFWriter open file. filename=[%s]", w.filename)
}

func (w *AOFWriter) Write(buf []byte) {
	_, err := w.file.Write(buf)
	if err != nil {
		log.PanicError(err)
	}
	w.offset += int64(len(buf))
	w.filesize += int64(len(buf))
	if w.filesize > MaxFileSize {
		w.Close()
		w.openFile(w.offset)
	}
	err = w.file.Sync()
	if err != nil {
		log.PanicError(err)
	}
}

func (w *AOFWriter) Close() {
	if w.file == nil {
		return
	}
	err := w.file.Sync()
	if err != nil {
		log.PanicError(err)
	}
	err = w.file.Close()
	if err != nil {
		log.PanicError(err)
	}
	log.Infof("AOFWriter close file. filename=[%s], filesize=[%d]", w.filename, w.filesize)
}
