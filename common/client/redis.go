package client

import (
	"bufio"
	"crypto/tls"
	"errors"
	"github.com/zhanghaiyang9999/RedisShake/common/client/proto"
	"github.com/zhanghaiyang9999/RedisShake/common/log"
	"net"
	"strconv"
	"time"
)

type Redis struct {
	reader      *bufio.Reader
	writer      *bufio.Writer
	protoReader *proto.Reader
	protoWriter *proto.Writer
}

func NewRedisClient(address string, username string, password string, isTls bool) (*Redis, error) {
	r := new(Redis)
	var conn net.Conn
	var dialer net.Dialer
	var err error
	dialer.Timeout = 3 * time.Second
	if isTls {
		conn, err = tls.DialWithDialer(&dialer, "tcp", address, &tls.Config{InsecureSkipVerify: true})
	} else {
		conn, err = dialer.Dial("tcp", address)
	}
	if err != nil {
		return r, err
	}

	r.reader = bufio.NewReader(conn)
	r.writer = bufio.NewWriter(conn)
	r.protoReader = proto.NewReader(r.reader)
	r.protoWriter = proto.NewWriter(r.writer)

	// auth
	if password != "" {
		var reply string
		var err error
		if username != "" {
			reply, err = r.DoWithStringReply("auth", username, password)
		} else {
			reply, err = r.DoWithStringReply("auth", password)
		}
		if reply != "OK" && err != nil {
			return r, err
		}
		log.Infof("auth successful. address=[%s]", address)
	} else {
		log.Infof("no password. address=[%s]", address)
	}

	// ping to test connection
	reply, err := r.DoWithStringReply("ping")

	if reply != "PONG" {
		return r, err
	}

	return r, nil
}

func (r *Redis) DoWithReply(args ...string) (interface{}, error) {
	err := r.Send(args...)
	if err != nil {
		return nil, err
	}
	replyInterface, err := r.Receive()
	return replyInterface, err
}
func (r *Redis) DoWithStringReply(args ...string) (string, error) {
	err := r.Send(args...)
	if err != nil {
		return "", err
	}
	replyInterface, err := r.Receive()
	if err != nil {
		return "", err
	}
	reply := replyInterface.(string)
	return reply, nil
}

func (r *Redis) Send(args ...string) error {
	argsInterface := make([]interface{}, len(args))
	for inx, item := range args {
		argsInterface[inx] = item
	}
	err := r.protoWriter.WriteArgs(argsInterface)
	if err != nil {
		return err
	}
	return r.flush()
}

func (r *Redis) SendBytes(buf []byte) error {
	_, err := r.writer.Write(buf)
	if err != nil {
		return err
	}
	return r.flush()
}

func (r *Redis) flush() error {
	err := r.writer.Flush()
	return err
}

func (r *Redis) Receive() (interface{}, error) {
	return r.protoReader.ReadReply()
}

func (r *Redis) BufioReader() *bufio.Reader {
	return r.reader
}

func (r *Redis) SetBufioReader(rd *bufio.Reader) {
	r.reader = rd
	r.protoReader = proto.NewReader(r.reader)
}

/* Commands */

func (r *Redis) Scan(cursor uint64) (newCursor uint64, keys []string, err error) {
	err = r.Send("scan", strconv.FormatUint(cursor, 10), "count", "2048")
	if err != nil {
		return
	}
	reply, err2 := r.Receive()
	if err2 != nil {
		err = err2
		return
	}
	array := reply.([]interface{})
	if len(array) != 2 {
		err = errors.New("reply is invalid")
		return
	}

	// cursor
	newCursor, err = strconv.ParseUint(array[0].(string), 10, 64)
	if err != nil {
		return
	}
	// keys
	keys = make([]string, 0)
	for _, item := range array[1].([]interface{}) {
		keys = append(keys, item.(string))
	}
	return
}
