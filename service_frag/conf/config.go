package conf

import (
	"github.com/jinzhu/configor"
)

var Config = &CustomerConfig{}

func InitConfig(file string) *CustomerConfig {
	if err := configor.Load(Config, file); err != nil {
		panic("failed to load local config file: ")
	}
	return Config
}

// ConfigNSQ sets the NSQ address
type ConfigNSQ struct {
	LookupAddress []string `yaml:"lookup_address_list"`
	NsqdAddress   string   `yaml:"addr"`
}

type CustomerConfig struct {
	Registry        ConfigEtcdOption `yaml:"etcd_option"`
	Nsqd            ConfigNSQ        `yaml:"nsq"`
	QuorumCap       int              `yaml:"quorum_cap"`        //集群数量
	IsMasterConsume bool             `yaml:"is_master_consume"` //主节点是否消费数据
	MasterTopic     []string         `yaml:"master_topic"`
}

type ConfigEtcdOption struct {
	Hosts        []string `yaml:"hosts"`
	ElectionPath string   `yaml:"election_path"`
	SlavePath    string   `yaml:"slave_path"`
	MasterPath   string   `yaml:"master_path"`
}
