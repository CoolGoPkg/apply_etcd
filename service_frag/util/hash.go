package util

import "hash/crc32"

func HashCode(s string) int { //固定的hashcode
	v := int(crc32.ChecksumIEEE([]byte(s)))
	if v >= 0 {
		return v
	}
	if -v >= 0 {
		return -v
	}
	// v == MinInt
	return 0
}

func GetIndex(code string, length, div int) int {
	hash := HashCode(code)
	index := hash % (length * div)
	return index
}
