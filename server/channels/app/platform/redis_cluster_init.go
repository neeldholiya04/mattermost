package platform

import (
	"github.com/mattermost/mattermost/server/public/shared/mlog"
	"github.com/mattermost/mattermost/server/v8/einterfaces"
)

// initRedisCluster initializes the Redis-based cluster implementation
func initRedisCluster(ps *PlatformService) einterfaces.ClusterInterface {
	// Create broadcast hooks map
	hooks := make(map[string]BroadcastHook)

	// Initialize Redis cluster with hooks
	rc := NewRedisCluster(ps, hooks)
	if rc == nil {
		return nil
	}

	if err := rc.Start(); err != nil {
		ps.logger.Error("Failed to start Redis cluster", mlog.Err(err))
		return nil
	}

	return rc
}
