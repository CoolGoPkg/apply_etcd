package util

import (
	"fmt"
	"strconv"
	"testing"
)

func TestGetIndex(t *testing.T) {

	hm := make(map[int][]string)
	for i := 0; i < 1000000; i++ {
		istr := strconv.Itoa(i) + ".BJ"
		index := GetIndex(istr, 6, 1)
		if _, ok := hm[index]; ok {
			hm[index] = append(hm[index], istr)
		} else {
			hm[index] = []string{istr}
		}
	}

	for k, _ := range hm {
		fmt.Printf("key : %d value : %#v \n\n", k, "sss")
	}
}
