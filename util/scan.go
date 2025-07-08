package util

import "bytes"

// 自定义分割函数，按分号分割
func SplitBySemicolon(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	if i := bytes.IndexByte(data, ';'); i >= 0 {
		// 找到分号，返回分号前的数据作为 token
		return i + 1, data[0:i], nil
	}
	if atEOF {
		// 文件末尾，返回剩余数据
		return len(data), data, nil
	}
	// 需要更多数据
	return 0, nil, nil
}
