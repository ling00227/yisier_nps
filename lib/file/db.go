package file

import (
	"crypto/md5"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"ehang.io/nps/lib/common"
	"ehang.io/nps/lib/crypt"
	"ehang.io/nps/lib/rate"
	"github.com/astaxie/beego/logs"
)

type ClusterInterface interface {
	Sync(op string, data interface{})
}

type DbUtils struct {
	JsonDb  *JsonDb
	Cluster ClusterInterface
}

var (
	Db   *DbUtils
	once sync.Once
	NewTaskHandler    func(t *Tunnel) error
	UpdateTaskHandler func(t *Tunnel) error
	DelTaskHandler    func(id int) error
)

// init csv from file
func GetDb() *DbUtils {
	once.Do(func() {
		jsonDb := NewJsonDb(common.GetRunPath())
		jsonDb.LoadClientFromJsonFile()
		jsonDb.LoadTaskFromJsonFile()
		jsonDb.LoadHostFromJsonFile()
		jsonDb.LoadGlobalFromJsonFile()
		Db = &DbUtils{JsonDb: jsonDb}
	})
	return Db
}

func GetMapKeys(m sync.Map, isSort bool, sortKey, order string) (keys []int) {
	if sortKey != "" && isSort {
		return sortClientByKey(m, sortKey, order)
	}
	m.Range(func(key, value interface{}) bool {
		keys = append(keys, key.(int))
		return true
	})
	sort.Ints(keys)
	return
}

func (s *DbUtils) GetClientList(start, length int, search, sort, order string, clientId int) ([]*Client, int) {
	list := make([]*Client, 0)
	var cnt int
	keys := GetMapKeys(s.JsonDb.Clients, true, sort, order)
	for _, key := range keys {
		if value, ok := s.JsonDb.Clients.Load(key); ok {
			v := value.(*Client)
			if v.NoDisplay {
				continue
			}
			if clientId != 0 && clientId != v.Id {
				continue
			}
			if search != "" && !(v.Id == common.GetIntNoErrByStr(search) || strings.Contains(v.VerifyKey, search) || strings.Contains(v.Remark, search)) {
				continue
			}
			cnt++
			if start--; start < 0 {
				if length--; length >= 0 {
					list = append(list, v)
				}
			}
		}
	}
	return list, cnt
}

func (s *DbUtils) GetIdByVerifyKey(vKey string, addr string) (id int, err error) {
	var exist bool
	s.JsonDb.Clients.Range(func(key, value interface{}) bool {
		v := value.(*Client)
		if common.Getverifyval(v.VerifyKey) == vKey && v.Status {
			v.Addr = common.GetIpByAddr(addr)
			id = v.Id
			exist = true
			return false
		}
		return true
	})
	if exist {
		return
	}
	return 0, errors.New("not found")
}

func (s *DbUtils) IncreaseVersion() {
	if s.JsonDb.Global == nil {
		s.JsonDb.Global = &Glob{}
	}
	s.JsonDb.Global.Lock()
	s.JsonDb.Global.Version = time.Now().UnixNano()
	s.JsonDb.Global.Unlock()
	s.JsonDb.StoreGlobalToJsonFile()
}

func (s *DbUtils) SetVersion(v int64) {
	if s.JsonDb.Global == nil {
		s.JsonDb.Global = &Glob{}
	}
	s.JsonDb.Global.Lock()
	if v > s.JsonDb.Global.Version {
		s.JsonDb.Global.Version = v
	}
	s.JsonDb.Global.Unlock()
	s.JsonDb.StoreGlobalToJsonFile()
}

func (s *DbUtils) NewTask(t *Tunnel) (err error) {
	return s.newTask(t, true, 0)
}

func (s *DbUtils) NewTaskFromCluster(t *Tunnel, version int64) (err error) {
	return s.newTask(t, false, version)
}

func (s *DbUtils) newTask(t *Tunnel, sync bool, version int64) (err error) {
	s.JsonDb.Tasks.Range(func(key, value interface{}) bool {
		v := value.(*Tunnel)
		if (v.Mode == "secret" || v.Mode == "p2p") && v.Password == t.Password {
			err = errors.New(fmt.Sprintf("secret mode keys %s must be unique", t.Password))
			return false
		}
		return true
	})
	if err != nil {
		return
	}
	if t.Client != nil {
		if client, err := s.GetClient(t.Client.Id); err == nil {
			t.Client = client
		}
	}
	if t.Flow == nil {
		t.Flow = new(Flow)
	}
	s.JsonDb.Tasks.Store(t.Id, t)
	s.JsonDb.StoreTasksToJsonFile()
	
	if sync {
		s.IncreaseVersion()
		if s.Cluster != nil {
			s.Cluster.Sync("NewTask", t)
		}
	} else {
		s.SetVersion(version)
		if NewTaskHandler != nil {
			if err := NewTaskHandler(t); err != nil {
				logs.Error("NewTaskHandler execution failed for task %d: %v", t.Id, err)
			} else {
				logs.Info("NewTaskHandler executed successfully for task %d", t.Id)
			}
		} else {
			logs.Warn("NewTaskHandler is nil for task %d", t.Id)
		}
	}
	return
}

func (s *DbUtils) UpdateTask(t *Tunnel) error {
	return s.updateTask(t, true, 0)
}

func (s *DbUtils) UpdateTaskFromCluster(t *Tunnel, version int64) error {
	return s.updateTask(t, false, version)
}

func (s *DbUtils) updateTask(t *Tunnel, sync bool, version int64) error {
	if t.Client != nil {
		if client, err := s.GetClient(t.Client.Id); err == nil {
			t.Client = client
		}
	}
	s.JsonDb.Tasks.Store(t.Id, t)
	s.JsonDb.StoreTasksToJsonFile()
	
	if sync {
		s.IncreaseVersion()
		if s.Cluster != nil {
			s.Cluster.Sync("UpdateTask", t)
		}
	} else {
		s.SetVersion(version)
		if UpdateTaskHandler != nil {
			if err := UpdateTaskHandler(t); err != nil {
				logs.Error("UpdateTaskHandler execution failed for task %d: %v", t.Id, err)
			} else {
				logs.Info("UpdateTaskHandler executed successfully for task %d", t.Id)
			}
		} else {
			logs.Warn("UpdateTaskHandler is nil for task %d", t.Id)
		}
	}
	return nil
}

func (s *DbUtils) UpdateGlobal(t *Glob) error {
	return s.updateGlobal(t, true, 0)
}

func (s *DbUtils) UpdateGlobalFromCluster(t *Glob, version int64) error {
	return s.updateGlobal(t, false, version)
}

func (s *DbUtils) updateGlobal(t *Glob, sync bool, version int64) error {
	s.JsonDb.Global = t
	s.JsonDb.StoreGlobalToJsonFile()
	
	if sync {
		s.IncreaseVersion()
		if s.Cluster != nil {
			s.Cluster.Sync("UpdateGlobal", t)
		}
	} else {
		s.SetVersion(version)
	}
	return nil
}

func (s *DbUtils) SaveGlobal(t *Glob) error {
	return s.UpdateGlobal(t)
}

func (s *DbUtils) DelTask(id int) error {
	return s.delTask(id, true, 0)
}

func (s *DbUtils) DelTaskFromCluster(id int, version int64) error {
	return s.delTask(id, false, version)
}

func (s *DbUtils) delTask(id int, sync bool, version int64) error {
	s.JsonDb.Tasks.Delete(id)
	s.JsonDb.StoreTasksToJsonFile()
	
	if sync {
		s.IncreaseVersion()
		if s.Cluster != nil {
			s.Cluster.Sync("DelTask", id)
		}
	} else {
		s.SetVersion(version)
		if DelTaskHandler != nil {
			DelTaskHandler(id)
		}
	}
	return nil
}

// md5 password
func (s *DbUtils) GetTaskByMd5Password(p string) (t *Tunnel) {
	s.JsonDb.Tasks.Range(func(key, value interface{}) bool {
		if crypt.Md5(value.(*Tunnel).Password) == p {
			t = value.(*Tunnel)
			return false
		}
		return true
	})
	return
}

func (s *DbUtils) GetTask(id int) (t *Tunnel, err error) {
	if v, ok := s.JsonDb.Tasks.Load(id); ok {
		t = v.(*Tunnel)
		return
	}
	err = errors.New("not found")
	return
}

func (s *DbUtils) DelHost(id int) error {
	return s.delHost(id, true, 0)
}

func (s *DbUtils) DelHostFromCluster(id int, version int64) error {
	return s.delHost(id, false, version)
}

func (s *DbUtils) delHost(id int, sync bool, version int64) error {
	s.JsonDb.Hosts.Delete(id)
	s.JsonDb.StoreHostToJsonFile()
	
	if sync {
		s.IncreaseVersion()
		if s.Cluster != nil {
			s.Cluster.Sync("DelHost", id)
		}
	} else {
		s.SetVersion(version)
	}
	return nil
}

func (s *DbUtils) IsHostExist(h *Host) bool {
	var exist bool
	s.JsonDb.Hosts.Range(func(key, value interface{}) bool {
		v := value.(*Host)
		if v.Id != h.Id && v.Host == h.Host && h.Location == v.Location && (v.Scheme == "all" || v.Scheme == h.Scheme) {
			exist = true
			return false
		}
		return true
	})
	return exist
}

func (s *DbUtils) NewHost(t *Host) error {
	return s.newHost(t, true, 0)
}

func (s *DbUtils) NewHostFromCluster(t *Host, version int64) error {
	return s.newHost(t, false, version)
}

func (s *DbUtils) newHost(t *Host, sync bool, version int64) error {
	if t.Location == "" {
		t.Location = "/"
	}
	if s.IsHostExist(t) {
		return errors.New("host has exist")
	}
	if t.Flow == nil {
		t.Flow = new(Flow)
	}
	s.JsonDb.Hosts.Store(t.Id, t)
	s.JsonDb.StoreHostToJsonFile()
	
	if sync {
		s.IncreaseVersion()
		if s.Cluster != nil {
			s.Cluster.Sync("NewHost", t)
		}
	} else {
		s.SetVersion(version)
	}
	return nil
}

func (s *DbUtils) UpdateHost(t *Host) error {
	return s.updateHost(t, true, 0)
}

func (s *DbUtils) UpdateHostFromCluster(t *Host, version int64) error {
	return s.updateHost(t, false, version)
}

func (s *DbUtils) updateHost(t *Host, sync bool, version int64) error {
	s.JsonDb.Hosts.Store(t.Id, t)
	s.JsonDb.StoreHostToJsonFile()
	
	if sync {
		s.IncreaseVersion()
		if s.Cluster != nil {
			s.Cluster.Sync("UpdateHost", t)
		}
	} else {
		s.SetVersion(version)
	}
	return nil
}

func (s *DbUtils) GetHost(start, length int, id int, search string) ([]*Host, int) {
	list := make([]*Host, 0)
	var cnt int
	keys := GetMapKeys(s.JsonDb.Hosts, false, "", "")
	for _, key := range keys {
		if value, ok := s.JsonDb.Hosts.Load(key); ok {
			v := value.(*Host)
			if search != "" && !(v.Id == common.GetIntNoErrByStr(search) || strings.Contains(v.Host, search) || strings.Contains(v.Remark, search) || strings.Contains(v.Client.VerifyKey, search)) {
				continue
			}
			if id == 0 || v.Client.Id == id {
				cnt++
				if start--; start < 0 {
					if length--; length >= 0 {
						list = append(list, v)
					}
				}
			}
		}
	}
	return list, cnt
}

func (s *DbUtils) DelClient(id int) error {
	return s.delClient(id, true, 0)
}

func (s *DbUtils) DelClientFromCluster(id int, version int64) error {
	return s.delClient(id, false, version)
}

func (s *DbUtils) delClient(id int, sync bool, version int64) error {
	s.JsonDb.Clients.Delete(id)
	s.JsonDb.StoreClientsToJsonFile()
	
	if sync {
		s.IncreaseVersion()
		if s.Cluster != nil {
			s.Cluster.Sync("DelClient", id)
		}
	} else {
		s.SetVersion(version)
	}
	return nil
}

func (s *DbUtils) NewClient(c *Client) error {
	return s.newClient(c, true, 0)
}

func (s *DbUtils) NewClientFromCluster(c *Client, version int64) error {
	return s.newClient(c, false, version)
}

func (s *DbUtils) NewClientNoSync(c *Client) error {
	return s.newClient(c, false, 0)
}

func (s *DbUtils) newClient(c *Client, sync bool, version int64) error {
	var isNotSet bool
	if c.Cnf == nil {
		c.Cnf = new(Config)
	}
	if c.WebUserName != "" && !s.VerifyUserName(c.WebUserName, c.Id) {
		return errors.New("web login username duplicate, please reset")
	}
reset:
	if c.VerifyKey == "" || isNotSet {
		isNotSet = true
		c.VerifyKey = crypt.GetVkey()
	}
	if c.RateLimit == 0 {
		c.Rate = rate.NewRate((2 << 23) * 1024)
	} else if c.Rate == nil {
		c.Rate = rate.NewRate(int64(c.RateLimit * 1024))
	}
	c.Rate.Start()
	if !s.VerifyVkey(c.VerifyKey, c.Id) {
		if isNotSet {
			goto reset
		}
		return errors.New("Vkey duplicate, please reset")
	}
	if c.Id == 0 {
		c.Id = int(s.JsonDb.GetClientId())
	}
	if c.Flow == nil {
		c.Flow = new(Flow)
	}
	s.JsonDb.Clients.Store(c.Id, c)
	s.JsonDb.StoreClientsToJsonFile()
	
	if sync {
		s.IncreaseVersion()
		if s.Cluster != nil {
			s.Cluster.Sync("NewClient", c)
		}
	} else {
		s.SetVersion(version)
	}
	return nil
}

func (s *DbUtils) VerifyVkey(vkey string, id int) (res bool) {
	res = true
	s.JsonDb.Clients.Range(func(key, value interface{}) bool {
		v := value.(*Client)
		if v.VerifyKey == vkey && v.Id != id {
			res = false
			return false
		}
		return true
	})
	return res
}

func (s *DbUtils) VerifyUserName(username string, id int) (res bool) {
	res = true
	s.JsonDb.Clients.Range(func(key, value interface{}) bool {
		v := value.(*Client)
		if v.WebUserName == username && v.Id != id {
			res = false
			return false
		}
		return true
	})
	return res
}

func (s *DbUtils) UpdateClient(t *Client) error {
	return s.updateClient(t, true, 0)
}

func (s *DbUtils) UpdateClientFromCluster(t *Client, version int64) error {
	return s.updateClient(t, false, version)
}

func (s *DbUtils) updateClient(t *Client, sync bool, version int64) error {
	s.JsonDb.Clients.Store(t.Id, t)
	if t.RateLimit == 0 {
		t.Rate = rate.NewRate(0)
		t.Rate.Start()
	}
	
	if sync {
		s.IncreaseVersion()
		if s.Cluster != nil {
			s.Cluster.Sync("UpdateClient", t)
		}
	} else {
		s.SetVersion(version)
	}
	return nil
}

func (s *DbUtils) IsPubClient(id int) bool {
	client, err := s.GetClient(id)
	if err == nil {
		return client.NoDisplay
	}
	return false
}

func (s *DbUtils) GetClient(id int) (c *Client, err error) {
	if v, ok := s.JsonDb.Clients.Load(id); ok {
		c = v.(*Client)
		return
	}
	err = errors.New("未找到客户端")
	return
}

func (s *DbUtils) GetGlobal() (c *Glob) {
	return s.JsonDb.Global
}

func (s *DbUtils) GetClientIdByVkey(vkey string) (id int, err error) {
	var exist bool
	s.JsonDb.Clients.Range(func(key, value interface{}) bool {
		v := value.(*Client)
		if crypt.Md5(v.VerifyKey) == vkey {
			exist = true
			id = v.Id
			return false
		}
		return true
	})
	if exist {
		return
	}
	err = errors.New("未找到客户端")
	return
}

func (s *DbUtils) GetClientByVkey(vkey string) (c *Client, err error) {
	var exist bool
	s.JsonDb.Clients.Range(func(key, value interface{}) bool {
		v := value.(*Client)
		if fmt.Sprintf("%x", md5.Sum([]byte(v.VerifyKey))) == vkey {
			exist = true
			c = v
			return false
		}
		return true
	})
	if exist {
		return
	}
	err = errors.New("未找到客户端")
	return
}

func (s *DbUtils) GetHostById(id int) (h *Host, err error) {
	if v, ok := s.JsonDb.Hosts.Load(id); ok {
		h = v.(*Host)
		return
	}
	err = errors.New("The host could not be parsed")
	return
}

// get key by host from x
func (s *DbUtils) GetInfoByHost(host string, r *http.Request) (h *Host, err error) {
	var hosts []*Host
	//Handling Ported Access
	host = common.GetIpByAddr(host)
	s.JsonDb.Hosts.Range(func(key, value interface{}) bool {
		v := value.(*Host)
		if v.IsClose {
			return true
		}
		//Remove http(s) http(s)://a.proxy.com
		//*.proxy.com *.a.proxy.com  Do some pan-parsing
		if v.Scheme != "all" && v.Scheme != r.URL.Scheme {
			return true
		}
		tmpHost := v.Host
		if strings.Contains(tmpHost, "*") {
			tmpHost = strings.Replace(tmpHost, "*", "", -1)
			if strings.Contains(host, tmpHost) {
				hosts = append(hosts, v)
			}
		} else if v.Host == host {
			hosts = append(hosts, v)
		}
		return true
	})

	for _, v := range hosts {
		//If not set, default matches all
		if v.Location == "" {
			v.Location = "/"
		}
		if strings.Index(r.RequestURI, v.Location) == 0 {
			if h == nil || (len(v.Location) > len(h.Location)) {
				h = v
			}
		}
	}
	if h != nil {
		return
	}
	err = errors.New("The host could not be parsed")
	return
}
