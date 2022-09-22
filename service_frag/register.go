package main

import (
	"CoolGoPkg/apply_etcd/service_frag/conf"
	"CoolGoPkg/apply_etcd/service_frag/util"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/pborman/uuid"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	serviceRegistryPrefix = "service"
)

var ServiceInstance *Service

type InstanceInfo struct {
	name         string
	leaseid      clientv3.LeaseID
	client       *clientv3.Client
	clusterId    uint64
	memberId     uint64
	ServicePath  string
	ElectionPath string
}

type QuorumInfo struct {
	QuorumCap       int //集群数量
	IsMasterConsume bool
}

type MasterInfo struct {
	Master         *atomic.Value
	Consumers      *LeaderConsumers
	watchNodesChan chan struct{}
}

type SlaveInfo struct {
	ChildrenNodeData *atomic.Value //订阅某个队列的数据
	Consumers        map[int]*FollowerConsumer
	once             sync.Once
}

type Service struct {
	instance *InstanceInfo
	quorum   *QuorumInfo
	master   *MasterInfo
	slave    *SlaveInfo
}

func NewService(config conf.ConfigEtcdOption, quormCap int, isMasterConsume bool) (*Service, error) {
	fmt.Println(config)
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   config.Hosts,
		DialTimeout: 2 * time.Second,
	})
	if err != nil {
		panic(err)
	}
	ServiceInstance = &Service{
		instance: &InstanceInfo{
			client:       cli,
			ServicePath:  config.SlavePath,
			ElectionPath: config.ElectionPath,
		},
		quorum: &QuorumInfo{
			QuorumCap:       quormCap,
			IsMasterConsume: isMasterConsume,
		},
		master: &MasterInfo{
			Master:         new(atomic.Value),
			watchNodesChan: make(chan struct{}),
		},
	}
	ServiceInstance.slave = ServiceInstance.InitSlaveInfo()
	ServiceInstance.master.Master.Store(false)
	ServiceInstance.Grant()
	ServiceInstance.instance.name = ServiceInstance.instance.ServicePath +
		"/" + serviceRegistryPrefix +
		"#" + util.GetInternal() +
		"#" + strconv.FormatInt(int64(ServiceInstance.instance.leaseid), 10)
	ServiceInstance.WatchSelfNodesData()
	go ServiceInstance.Election()
	return ServiceInstance, nil
}

func (s *Service) InitSlaveInfo() *SlaveInfo {
	return &SlaveInfo{
		ChildrenNodeData: new(atomic.Value),
		Consumers:        make(map[int]*FollowerConsumer),
	}
}

func (s *Service) Start() error {
	ch, err := s.keepAlive(s.instance.name, s.instance.leaseid)
	if err != nil {
		fmt.Println(err)
		return err
	}
	for {
		select {
		case <-s.instance.client.Ctx().Done():
			return errors.New("server closed")
		case _, ok := <-ch:
			if !ok {
				fmt.Println("keep alive channel closed")
				s.revoke()
				return nil
			}
			//else {
			//log.Printf("Recv reply from service: %s, ttl:%d ", s.Name, ka.TTL, s.leaseid, ka.ID, ka.ClusterId, ka.MemberId, ka.Revision)
			//}
		}
	}
}

func (s *Service) revoke() error {
	_, err := s.instance.client.Revoke(context.TODO(), s.instance.leaseid)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Printf("servide:%s stop \n", s.instance.name)
	return err
}

func (s *Service) keepAlive(key string, id clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	_, err := s.instance.client.Put(context.TODO(), key, string([]byte{}), clientv3.WithLease(id))
	if err != nil {
		fmt.Println(err)
		panic(err)
	}
	return s.instance.client.KeepAlive(context.TODO(), id)
}

func (s *Service) Grant() {
	resp, err := s.instance.client.Grant(context.TODO(), 2)
	if err != nil {
		fmt.Println("create lease err : ", err)
		panic(err)
	}
	s.instance.leaseid = resp.ID
	s.instance.clusterId = resp.ClusterId
	s.instance.memberId = resp.MemberId
}

func (s *Service) WatchSelfNodesData() {
	rch := s.instance.client.Watch(context.Background(), s.instance.name, clientv3.WithKeysOnly())
	go func() {
		for wresp := range rch {
			for _, ev := range wresp.Events {
				switch ev.Type {
				case clientv3.EventTypePut:
					fmt.Println("watch node data put ", ev.IsModify(), string(ev.Kv.Key), string(ev.Kv.Value))
					if ev.IsModify() {
						if ev.Kv == nil || len(ev.Kv.Value) == 0 {
							continue
						}
						data := make([]string, 0)
						err := json.Unmarshal(ev.Kv.Value, &data)
						if err != nil {
							fmt.Println("node data deserial occured err::", err)
							continue
						}
						value := s.slave.ChildrenNodeData.Load()
						fmt.Println("之前监控的节点为:", value)
						fmt.Println("目前新监控的节点为:", data)

						s.slave.ChildrenNodeData.Store(data)
						if value == nil {
							s.startConsume(data)
						} else {
							oldIndexes := value.([]string)
							newIndexMap := make(map[string]bool, 0)
							for _, v := range data {
								newIndexMap[v] = true
							}
							for _, v := range oldIndexes {
								if _, ok := newIndexMap[v]; !ok {
									s.stopConsume(v)
								}
							}
							s.startConsume(data)
						}
					}
				case clientv3.EventTypeDelete:
					fmt.Println("watch node data delete ", ev.IsModify(), string(ev.Kv.Key), string(ev.Kv.Value))
					if s.master.Master.Load().(bool) {
						s.master.Master.Store(false)
						s.master.Consumers.Stop()
						s.master.watchNodesChan <- struct{}{}
						go s.Election()
					}
					//重连
					s.instance.client.Put(context.TODO(), s.instance.name, string([]byte{}), clientv3.WithLease(s.instance.leaseid))
				}
			}
		}
	}()
}

func (s *Service) WatchNodes() {
	rch := s.instance.client.Watch(context.Background(), s.instance.ServicePath, clientv3.WithPrefix())
	s.distributedNodeJob()
	go func() {
		for {
			select {
			case wresp := <-rch:
				for _, ev := range wresp.Events {
					switch ev.Type {
					case clientv3.EventTypePut:
						fmt.Println("watch node put ", ev.IsCreate(), string(ev.Kv.Key), string(ev.Kv.Value))
						if ev.IsCreate() {
							s.distributedNodeJob()
						}
					case clientv3.EventTypeDelete:
						fmt.Println("watch node delete  ", string(ev.Kv.Key), string(ev.Kv.Value))
						if s.master.Master.Load().(bool) && string(ev.Kv.Key) != s.instance.name {
							s.distributedNodeJob()
						}
					}
				}
			case <-s.master.watchNodesChan:
				return
			}
		}
	}()
}

func (s *Service) Election() {
	s1, err := concurrency.NewSession(s.instance.client, concurrency.WithTTL(2))
	if err != nil {
		fmt.Println("new session:", err)
		panic(err)
	}
	e1 := concurrency.NewElection(s1, s.instance.ElectionPath)
	if err := e1.Campaign(context.Background(), util.GetInternal()+"_"+uuid.New()); err != nil {
		fmt.Println("campaign :", err)
		panic(err)
	}
	cctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	fmt.Println("elect success info,", (<-e1.Observe(cctx)), s.instance.name)
	s.master.Master.Store(true)
	s.StopSlaveOperator()
	s.master.Consumers = InitLeaderConsumer(conf.Config.MasterTopic, conf.Config.Nsqd)
	s.WatchNodes()
}

func (s *Service) StopSlaveOperator() {
	value := s.slave.ChildrenNodeData.Load() //停止消费follower_consumer
	if value != nil {
		indexes := value.([]string)
		if len(indexes) > 0 {
			for _, index := range indexes {
				s.stopConsume(index)
			}
		}
	}

	s.slave = s.InitSlaveInfo()
}

func (s *Service) startConsume(indexes []string) {

	for _, index := range indexes {
		index, _ := strconv.Atoi(index)
		if _, ok := s.slave.Consumers[index]; !ok {
			s.slave.Consumers[index] = InitFollowerConsumer(index, conf.Config.Nsqd)
		}
	}
}

func (s *Service) stopConsume(indexNode string) {
	index, _ := strconv.Atoi(indexNode)
	if consumer, ok := s.slave.Consumers[index]; ok {
		consumer.Stop()
		delete(s.slave.Consumers, index)
	}
}

func (s *Service) distributedNodeJob() {
	keys, err := s.getNodeInfo()
	fmt.Println("keys:::", keys)
	if err != nil {
		fmt.Println("get node info occured error,", err)
		return
	}
	data := s.AssignNode(keys)
	s.LoadNodeToEtcd(data)
}

func (s *Service) getNodeInfo() ([]string, error) {
	response, err := s.instance.client.Get(context.Background(), s.instance.ServicePath, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByCreateRevision, clientv3.SortAscend))
	if err != nil {
		fmt.Println("err::", err)
		return nil, err
	}
	//重新分发
	if response == nil || response.Kvs == nil || len(response.Kvs) == 0 {
		return nil, errors.New("err node is not found")
	}
	resp := make([]string, 0, len(response.Kvs))
	for _, v := range response.Kvs {
		resp = append(resp, string(v.Key))
	}
	return resp, nil
}

func (s *Service) AssignNode(keys []string) map[string][]string {
	distributed := make(map[string][]string)
	quorumCap := s.quorum.QuorumCap
	if !s.quorum.IsMasterConsume {
		quorumCap--
	}
	s.assignNode(quorumCap, keys, distributed)
	return distributed
}

func (s *Service) assignNode(quorumCap int, keys []string, distributed map[string][]string) {
	if quorumCap == 0 {
		return
	}
	if s.quorum.IsMasterConsume {
		if len(keys) == 0 {
			return
		}
	} else {
		if len(keys) == 1 {
			return
		}
	}
	for _, path := range keys {
		val := s.master.Master.Load()
		if val == nil {
			fmt.Println("还没有master，暂时不分发数据，略过...........................")
			return
		}
		master := val.(bool)
		if master && !s.quorum.IsMasterConsume && path == s.instance.name {
			fmt.Println("主节点不消费数据，略过...........................")
			continue
		}
		distributed[path] = append(distributed[path], strconv.Itoa(quorumCap-1))
		quorumCap--
		if quorumCap <= 0 {
			return
		}
	}
	s.assignNode(quorumCap, keys, distributed)
}

func (s *Service) LoadNodeToEtcd(data map[string][]string) {
	for key, value := range data {
		fmt.Println("set node data %s -- %v \n", key, value)
		s.SetData(key, value)
	}
}

func (s *Service) SetData(path string, data interface{}) {
	pathArray := strings.Split(path, "#")
	leaseId, err := strconv.ParseInt(pathArray[2], 10, 64)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	_, err = s.instance.client.Put(ctx, path, util.FromObject(data), clientv3.WithLease(clientv3.LeaseID(leaseId)))
	cancel()
	if err != nil {
		fmt.Println("put failed, err:", err)
		return
	}
}
