package util

import (
	"encoding/json"
	"net"
	"os"
)

func FromObject(obj interface{}) string {
	data, _ := json.Marshal(obj)
	return string(data)
}

const localIp = "127.0.0.1"

func GetInternal() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		os.Stderr.WriteString("Oops:" + err.Error())
		return localIp
	}
	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return localIp
}
