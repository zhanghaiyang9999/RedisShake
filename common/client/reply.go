package client

import "github.com/zhanghaiyang9999/RedisShake/common/log"

func ArrayString(replyInterface interface{}, err error) ([]string, error) {
	if err != nil {
		return make([]string, 1), err
	}
	replyArray := replyInterface.([]interface{})
	replyArrayString := make([]string, len(replyArray))
	for inx, item := range replyArray {
		replyArrayString[inx] = item.(string)
	}
	return replyArrayString, nil
}

func String(reply interface{}, err error) (string, error) {
	if err != nil {
		return "", err
	}
	return reply.(string), err
}

func Int64(reply interface{}, err error) (int64, error) {
	if err != nil {
		return 0, err
	}
	switch v := reply.(type) {
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	default:
		log.Panicf("reply type is not int64 or int, type=%T", v)
	}
	return 0, err
}
