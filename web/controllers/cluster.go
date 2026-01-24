package controllers

import (
	"ehang.io/nps/lib/file"
	"ehang.io/nps/server/cluster"
	"github.com/astaxie/beego"
)

type ClusterController struct {
	BaseController
}

func (s *ClusterController) Index() {
	s.SetInfo("cluster")
	s.Data["web_base_url"] = beego.AppConfig.String("web_base_url")
	if cluster.Manager == nil {
		s.Data["enabled"] = false
	} else {
		s.Data["enabled"] = true
		s.Data["myAddr"] = cluster.Manager.MyAddr
		
		cluster.Manager.StatusLock.RLock()
		s.Data["peerStatuses"] = cluster.Manager.PeerStatuses
		cluster.Manager.StatusLock.RUnlock()

		if file.GetDb().JsonDb.Global != nil {
			s.Data["version"] = file.GetDb().JsonDb.Global.Version
		} else {
			s.Data["version"] = 0
		}
	}
	s.display()
}

func (s *ClusterController) Sync() {
	password := s.GetString("password")
	if password != beego.AppConfig.String("web_password") {
		s.Data["json"] = map[string]interface{}{"status": 0, "msg": "Password incorrect"}
		s.ServeJSON()
		return
	}

	if cluster.Manager != nil {
		cluster.Manager.SyncFromPeers(true)
		s.Data["json"] = map[string]interface{}{"status": 1, "msg": "Sync triggered"}
	} else {
		s.Data["json"] = map[string]interface{}{"status": 0, "msg": "Cluster not enabled"}
	}
	s.ServeJSON()
}
