package cluster

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"ehang.io/nps/lib/file"
	"github.com/astaxie/beego"
	"github.com/astaxie/beego/logs"
)

type PeerStatus struct {
	Address  string
	IsOnline bool
	LastSeen time.Time
	Version  int64
	Latency  string
	Error    string
}

type ClusterManager struct {
	Peers        []string
	Secret       string
	Client       *http.Client
	MyAddr       string
	PeerStatuses map[string]*PeerStatus
	StatusLock   sync.RWMutex
}

var Manager *ClusterManager

func InitCluster() {
	if !beego.AppConfig.DefaultBool("cluster_mode", false) {
		return
	}

	peers := strings.Split(beego.AppConfig.String("cluster_peers"), ",")
	peerStatuses := make(map[string]*PeerStatus)
	for _, p := range peers {
		if p == "" {
			continue
		}
		peerStatuses[p] = &PeerStatus{
			Address:  p,
			IsOnline: false,
		}
	}

	Manager = &ClusterManager{
		Peers:        peers,
		Secret:       strings.TrimSpace(beego.AppConfig.String("cluster_secret")),
		Client:       &http.Client{Timeout: 5 * time.Second},
		MyAddr:       fmt.Sprintf("%s:%s", beego.AppConfig.String("cluster_ip"), beego.AppConfig.String("cluster_port")),
		PeerStatuses: peerStatuses,
	}

	// Register to DbUtils
	file.GetDb().Cluster = Manager

	go Manager.Start()
}

func (c *ClusterManager) Start() {
	// Start HTTP Server
	port := beego.AppConfig.String("cluster_port")
	mux := http.NewServeMux()
	mux.HandleFunc("/sync", c.handleSync)
	mux.HandleFunc("/version", c.handleVersion)
	mux.HandleFunc("/dump", c.handleDump)

	// Start Health Check
	go c.StartHealthCheck()

	// Sync from peers on startup
	go func() {
		time.Sleep(5 * time.Second) // Wait for other services
		c.SyncFromPeers(false)
	}()

	logs.Info("Cluster manager starting on port %s", port)
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		logs.Error("Cluster manager start failed: %v", err)
	}
}

func (c *ClusterManager) AddPeer(addr string) {
	c.StatusLock.Lock()
	defer c.StatusLock.Unlock()

	// Check if already exists
	if _, ok := c.PeerStatuses[addr]; ok {
		return
	}

	// Add to map
	c.PeerStatuses[addr] = &PeerStatus{
		Address:  addr,
		IsOnline: true,
		LastSeen: time.Now(),
	}

	// Add to slice if not present
	exists := false
	for _, p := range c.Peers {
		if p == addr {
			exists = true
			break
		}
	}
	if !exists {
		c.Peers = append(c.Peers, addr)
		logs.Info("New peer discovered: %s", addr)
	}
}

func (c *ClusterManager) StartHealthCheck() {
	ticker := time.NewTicker(30 * time.Second)
	for range ticker.C {
		c.SyncFromPeers(false)
	}
}

func (c *ClusterManager) checkPeers(wait bool) {
	// Copy peers to avoid holding lock during IO
	c.StatusLock.RLock()
	peers := make([]string, len(c.Peers))
	copy(peers, c.Peers)
	c.StatusLock.RUnlock()

	var wg sync.WaitGroup
	for _, peer := range peers {
		if peer == "" || peer == c.MyAddr {
			continue
		}
		if wait {
			wg.Add(1)
		}
		go func(p string) {
			if wait {
				defer wg.Done()
			}
			c.checkPeer(p)
		}(peer)
	}
	if wait {
		wg.Wait()
	}
}

func (c *ClusterManager) checkPeer(peer string) {
	start := time.Now()
	req, _ := http.NewRequest("GET", fmt.Sprintf("http://%s/version", peer), nil)
	req.Header.Set("Secret", c.Secret)
	req.Header.Set("X-NPS-Peer-Addr", c.MyAddr)
	// Add port header if MyAddr is 0.0.0.0
	if strings.HasPrefix(c.MyAddr, "0.0.0.0:") {
		port := strings.Split(c.MyAddr, ":")[1]
		req.Header.Set("X-NPS-Peer-Port", port)
	}

	resp, err := c.Client.Do(req)

	c.StatusLock.Lock()
	defer c.StatusLock.Unlock()

	status, ok := c.PeerStatuses[peer]
	if !ok {
		status = &PeerStatus{Address: peer}
		c.PeerStatuses[peer] = status
	}

	if err != nil {
		logs.Warn("Cluster check peer %s failed: %v", peer, err)
		status.IsOnline = false
		status.Error = err.Error()
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		logs.Warn("Cluster check peer %s returned status %d", peer, resp.StatusCode)
		status.IsOnline = false
		status.Error = fmt.Sprintf("HTTP Status %d", resp.StatusCode)
		return
	}

	var res struct {
		Version int64 `json:"version"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&res); err == nil {
		status.IsOnline = true
		status.Error = ""
		status.LastSeen = time.Now()
		status.Version = res.Version
		status.Latency = time.Since(start).String()
	} else {
		logs.Warn("Cluster check peer %s decode failed: %v", peer, err)
		status.IsOnline = false
		status.Error = fmt.Sprintf("Decode failed: %v", err)
	}
}

type SyncPayload struct {
	Op       string      `json:"op"`
	Data     interface{} `json:"data"`
	Secret   string      `json:"secret"`
	Version  int64       `json:"version"`
	FromAddr string      `json:"from_addr"`
}

func (c *ClusterManager) Sync(op string, data interface{}) {
	payload := SyncPayload{
		Op:       op,
		Data:     data,
		Secret:   c.Secret,
		Version:  file.GetDb().JsonDb.Global.Version,
		FromAddr: c.MyAddr,
	}

	// Copy peers to avoid holding lock during IO
	c.StatusLock.RLock()
	peers := make([]string, len(c.Peers))
	copy(peers, c.Peers)
	c.StatusLock.RUnlock()

	// Broadcast
	for _, peer := range peers {
		if peer == "" || peer == c.MyAddr {
			continue
		}
		go c.sendToPeer(peer, payload)
	}
}

func (c *ClusterManager) sendToPeer(peer string, payload SyncPayload) {
	// Fix FromAddr if 0.0.0.0
	if strings.HasPrefix(payload.FromAddr, "0.0.0.0:") {
		// Use empty string to indicate peer should use RemoteAddr + X-NPS-Peer-Port
		payload.FromAddr = "" 
	}

	b, _ := json.Marshal(payload)
	req, _ := http.NewRequest("POST", fmt.Sprintf("http://%s/sync", peer), bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	
	// Add port header if MyAddr is 0.0.0.0
	if strings.HasPrefix(c.MyAddr, "0.0.0.0:") {
		port := strings.Split(c.MyAddr, ":")[1]
		req.Header.Set("X-NPS-Peer-Port", port)
	}

	resp, err := c.Client.Do(req)
	if err != nil {
		logs.Warn("Failed to sync to peer %s: %v", peer, err)
		return
	}
	defer resp.Body.Close()
}

func (c *ClusterManager) extractPeerAddr(r *http.Request, fromAddr string) string {
	// If fromAddr is valid and not 0.0.0.0, use it
	if fromAddr != "" && !strings.HasPrefix(fromAddr, "0.0.0.0:") {
		return fromAddr
	}

	// Try headers
	headerAddr := r.Header.Get("X-NPS-Peer-Addr")
	if headerAddr != "" && !strings.HasPrefix(headerAddr, "0.0.0.0:") {
		return headerAddr
	}

	// Construct from RemoteAddr and Port header
	remoteIP := strings.Split(r.RemoteAddr, ":")[0]
	// Handle IPv6 [::1] format if needed, but Split works for standard IPv4:Port
	// For IPv6 it might be [ip]:port.
	if strings.Contains(r.RemoteAddr, "]:") {
		remoteIP = strings.Split(r.RemoteAddr, "]:")[0] + "]"
	}

	port := r.Header.Get("X-NPS-Peer-Port")
	if port != "" {
		return fmt.Sprintf("%s:%s", remoteIP, port)
	}

	return ""
}

func (c *ClusterManager) handleSync(w http.ResponseWriter, r *http.Request) {
	// Redefine payload struct for decoding
	type DecodePayload struct {
		Op       string          `json:"op"`
		Data     json.RawMessage `json:"data"`
		Secret   string          `json:"secret"`
		Version  int64           `json:"version"`
		FromAddr string          `json:"from_addr"`
	}
	var dp DecodePayload
	if err := json.NewDecoder(r.Body).Decode(&dp); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}

	if dp.Secret != c.Secret {
		logs.Warn("Cluster sync auth failed. Peer %s sent secret '%s', expected '%s'", r.RemoteAddr, dp.Secret, c.Secret)
		http.Error(w, "Unauthorized", 401)
		return
	}

	// Dynamic Discovery
	peerAddr := c.extractPeerAddr(r, dp.FromAddr)
	if peerAddr != "" {
		c.AddPeer(peerAddr)
	}

	// Apply change
	db := file.GetDb()

	switch dp.Op {
	case "NewTask", "UpdateTask":
		var t file.Tunnel
		json.Unmarshal(dp.Data, &t)
		if dp.Op == "NewTask" {
			db.NewTaskFromCluster(&t, dp.Version)
		} else {
			db.UpdateTaskFromCluster(&t, dp.Version)
		}
	case "DelTask":
		var id int
		json.Unmarshal(dp.Data, &id)
		db.DelTaskFromCluster(id, dp.Version)
	case "NewClient", "UpdateClient":
		var cl file.Client
		json.Unmarshal(dp.Data, &cl)
		if dp.Op == "NewClient" {
			db.NewClientFromCluster(&cl, dp.Version)
		} else {
			db.UpdateClientFromCluster(&cl, dp.Version)
		}
	case "DelClient":
		var id int
		json.Unmarshal(dp.Data, &id)
		db.DelClientFromCluster(id, dp.Version)
	case "NewHost", "UpdateHost":
		var h file.Host
		json.Unmarshal(dp.Data, &h)
		if dp.Op == "NewHost" {
			db.NewHostFromCluster(&h, dp.Version)
		} else {
			db.UpdateHostFromCluster(&h, dp.Version)
		}
	case "DelHost":
		var id int
		json.Unmarshal(dp.Data, &id)
		db.DelHostFromCluster(id, dp.Version)
	case "UpdateGlobal":
		var g file.Glob
		json.Unmarshal(dp.Data, &g)
		db.UpdateGlobalFromCluster(&g, dp.Version)
	}
}

func (c *ClusterManager) handleVersion(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Secret") != c.Secret {
		logs.Warn("Cluster version auth failed. Peer %s sent secret '%s', expected '%s'", r.RemoteAddr, r.Header.Get("Secret"), c.Secret)
		http.Error(w, "Unauthorized", 401)
		return
	}

	// Dynamic Discovery
	peerAddr := c.extractPeerAddr(r, "")
	if peerAddr != "" {
		c.AddPeer(peerAddr)
	}

	v := int64(0)
	if file.GetDb().JsonDb.Global != nil {
		v = file.GetDb().JsonDb.Global.Version
	}
	fmt.Fprintf(w, `{"version": %d}`, v)
}

func (c *ClusterManager) handleDump(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Secret") != c.Secret {
		logs.Warn("Cluster dump auth failed. Peer %s sent secret '%s', expected '%s'", r.RemoteAddr, r.Header.Get("Secret"), c.Secret)
		http.Error(w, "Unauthorized", 401)
		return
	}

	// Dynamic Discovery
	peerAddr := c.extractPeerAddr(r, "")
	if peerAddr != "" {
		c.AddPeer(peerAddr)
	}

	data := make(map[string]interface{})

	tasks := make([]*file.Tunnel, 0)
	file.GetDb().JsonDb.Tasks.Range(func(k, v interface{}) bool {
		tasks = append(tasks, v.(*file.Tunnel))
		return true
	})
	data["tasks"] = tasks

	clients := make([]*file.Client, 0)
	file.GetDb().JsonDb.Clients.Range(func(k, v interface{}) bool {
		clients = append(clients, v.(*file.Client))
		return true
	})
	data["clients"] = clients

	hosts := make([]*file.Host, 0)
	file.GetDb().JsonDb.Hosts.Range(func(k, v interface{}) bool {
		hosts = append(hosts, v.(*file.Host))
		return true
	})
	data["hosts"] = hosts

	if file.GetDb().JsonDb.Global != nil {
		data["global"] = file.GetDb().JsonDb.Global
	}

	json.NewEncoder(w).Encode(data)
}

func (c *ClusterManager) SyncFromPeers(force bool) {
	c.checkPeers(true) // Initial check and wait for results

	var maxVer int64
	var bestPeer string

	c.StatusLock.RLock()
	for peer, status := range c.PeerStatuses {
		if status.IsOnline && status.Version > maxVer {
			maxVer = status.Version
			bestPeer = peer
		}
	}
	c.StatusLock.RUnlock()

	myVer := int64(0)
	if file.GetDb().JsonDb.Global != nil {
		myVer = file.GetDb().JsonDb.Global.Version
	}

	if force {
		if bestPeer != "" {
			logs.Info("Force syncing from peer %s (ver: %d)", bestPeer, maxVer)
			c.doFullSync(bestPeer)
		} else {
			logs.Warn("Force sync failed: no online peers found")
		}
		return
	}

	if maxVer > myVer {
		logs.Info("Syncing from peer %s (ver: %d > %d)", bestPeer, maxVer, myVer)
		c.doFullSync(bestPeer)
	}
}

func (c *ClusterManager) doFullSync(peer string) {
	req, _ := http.NewRequest("GET", fmt.Sprintf("http://%s/dump", peer), nil)
	req.Header.Set("Secret", c.Secret)
	
	// Add Dynamic Discovery Headers
	req.Header.Set("X-NPS-Peer-Addr", c.MyAddr)
	if strings.HasPrefix(c.MyAddr, "0.0.0.0:") {
		port := strings.Split(c.MyAddr, ":")[1]
		req.Header.Set("X-NPS-Peer-Port", port)
	}

	resp, err := c.Client.Do(req)
	if err != nil {
		logs.Error("Full sync failed: %v", err)
		return
	}
	defer resp.Body.Close()

	var data struct {
		Tasks   []*file.Tunnel `json:"tasks"`
		Clients []*file.Client `json:"clients"`
		Hosts   []*file.Host   `json:"hosts"`
		Global  *file.Glob     `json:"global"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		logs.Error("Full sync decode failed: %v", err)
		return
	}

	db := file.GetDb()

	db.JsonDb.Tasks.Range(func(key, value interface{}) bool {
		db.JsonDb.Tasks.Delete(key)
		return true
	})
	for _, t := range data.Tasks {
		db.JsonDb.Tasks.Store(t.Id, t)
	}
	db.JsonDb.StoreTasksToJsonFile()

	db.JsonDb.Clients.Range(func(key, value interface{}) bool {
		db.JsonDb.Clients.Delete(key)
		return true
	})
	for _, c := range data.Clients {
		db.JsonDb.Clients.Store(c.Id, c)
	}
	db.JsonDb.StoreClientsToJsonFile()

	db.JsonDb.Hosts.Range(func(key, value interface{}) bool {
		db.JsonDb.Hosts.Delete(key)
		return true
	})
	for _, h := range data.Hosts {
		db.JsonDb.Hosts.Store(h.Id, h)
	}
	db.JsonDb.StoreHostToJsonFile()

	if data.Global != nil {
		db.JsonDb.Global = data.Global
		db.JsonDb.StoreGlobalToJsonFile()
	}

	logs.Info("Full sync completed")
}
