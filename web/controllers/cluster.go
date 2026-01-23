package controllers

import (
	"ehang.io/nps/lib/file"
	"ehang.io/nps/server/cluster"
)

type ClusterController struct {
	BaseController
}

func (s *ClusterController) Index() {
	s.SetInfo("cluster")
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
