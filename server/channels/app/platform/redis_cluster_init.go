package platform

import (
	"github.com/mattermost/mattermost/server/v8/einterfaces"
)

// initRedisCluster initializes the Redis-based cluster implementation
func initRedisCluster(ps *PlatformService) einterfaces.ClusterInterface {
	if ps.Config().ClusterSettings.Enable != nil && *ps.Config().ClusterSettings.Enable {
		if *ps.Config().CacheSettings.CacheType == "redis" {
			return NewRedisCluster(ps)
		}
	}
	return nil
}
