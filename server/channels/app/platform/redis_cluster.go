// server/channels/app/platform/redis_cluster.go

package platform

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/mattermost/mattermost/server/public/model"
	"github.com/mattermost/mattermost/server/public/shared/mlog"
	"github.com/mattermost/mattermost/server/public/shared/request"
	"github.com/mattermost/mattermost/server/v8/einterfaces"
)

const (
	redisPubSubChannel = "mattermost_cluster"
	redisLeaderKey     = "mattermost_cluster_leader"
	redisNodesKey      = "mattermost_cluster_nodes"
	redisEventChannel  = "mattermost_cluster_events"
	leaderTTL          = 15 * time.Second
	nodesTTL           = 20 * time.Second
	heartbeatInterval  = 5 * time.Second
	maxMessageBuffer   = 1000
	syncTimeout        = 10 * time.Second
)

type redisCluster struct {
	ps            *PlatformService
	nodeID        string
	rdb           *redis.Client
	pubsub        *redis.PubSub
	handlers      map[model.ClusterEvent][]einterfaces.ClusterMessageHandler
	handlersMutex sync.RWMutex
	isLeader      atomic.Bool
	leaderMutex   sync.RWMutex
	stopChan      chan struct{}
	wg            sync.WaitGroup
	logger        *mlog.Logger
	isReady       atomic.Bool
	messageBuffer chan model.ClusterMessage
	ctx           context.Context
	cancel        context.CancelFunc
	lastSync      atomic.Int64
	syncVersion   atomic.Int64
}

func NewRedisCluster(ps *PlatformService) *redisCluster {
	nodeID := model.NewId()
	ctx, cancel := context.WithCancel(context.Background())

	rc := &redisCluster{
		ps:            ps,
		nodeID:        nodeID,
		handlers:      make(map[model.ClusterEvent][]einterfaces.ClusterMessageHandler),
		stopChan:      make(chan struct{}),
		logger:        ps.logger.With(mlog.String("cluster_node_id", nodeID)),
		messageBuffer: make(chan model.ClusterMessage, maxMessageBuffer),
		ctx:           ctx,
		cancel:        cancel,
	}

	cfg := ps.Config().CacheSettings
	rc.rdb = redis.NewClient(&redis.Options{
		Addr:     *cfg.RedisAddress,
		Password: *cfg.RedisPassword,
		DB:       int(*cfg.RedisDB),
	})

	return rc
}

func (rc *redisCluster) Start() error {
	// Test Redis connection with timeout
	ctx, cancel := context.WithTimeout(rc.ctx, 5*time.Second)
	defer cancel()

	if err := rc.rdb.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("failed to connect to Redis: %w", err)
	}

	// Subscribe to cluster events
	rc.pubsub = rc.rdb.Subscribe(rc.ctx, redisPubSubChannel, redisEventChannel)

	// Start cluster routines
	rc.wg.Add(5)
	go rc.leaderElectionLoop()
	go rc.heartbeatLoop()
	go rc.messageListener()
	go rc.messageProcessor()
	go rc.syncLoop()

	rc.logger.Info("Redis cluster started",
		mlog.String("node_id", rc.nodeID),
		mlog.String("redis_address", *rc.ps.Config().CacheSettings.RedisAddress))
	return nil
}

func (rc *redisCluster) Stop() {
	rc.cancel()
	close(rc.stopChan)
	if rc.pubsub != nil {
		rc.pubsub.Close()
	}
	rc.wg.Wait()
	rc.logger.Info("Redis cluster stopped", mlog.String("node_id", rc.nodeID))
}

func (rc *redisCluster) leaderElectionLoop() {
	defer rc.wg.Done()
	ticker := time.NewTicker(leaderTTL / 2)
	defer ticker.Stop()

	for {
		select {
		case <-rc.ctx.Done():
			return
		case <-ticker.C:
			rc.tryBecomeLeader()
		}
	}
}

func (rc *redisCluster) tryBecomeLeader() {
	ctx, cancel := context.WithTimeout(rc.ctx, 2*time.Second)
	defer cancel()

	wasLeader := rc.IsLeader()

	// Use multi to ensure atomicity
	tx := rc.rdb.TxPipeline()

	// Try to become leader
	tx.SetNX(ctx, redisLeaderKey, rc.nodeID, leaderTTL)

	// Set sync version
	tx.Set(ctx, fmt.Sprintf("%s:sync:%s", redisNodesKey, rc.nodeID), rc.syncVersion.Load(), nodesTTL)

	cmds, err := tx.Exec(ctx)
	if err != nil {
		rc.logger.Error("Failed to perform leader election",
			mlog.Err(err),
			mlog.String("node_id", rc.nodeID))
		return
	}

	success := cmds[0].(*redis.BoolCmd).Val()
	rc.isLeader.Store(success)

	if wasLeader != success {
		if success {
			rc.logger.Info("Became cluster leader", mlog.String("node_id", rc.nodeID))
			// Trigger immediate sync when becoming leader
			go rc.verifyClusterSync()
		} else {
			rc.logger.Info("Lost cluster leadership", mlog.String("node_id", rc.nodeID))
		}
		rc.ps.InvokeClusterLeaderChangedListeners()
	}

	if success {
		if err := rc.rdb.Expire(ctx, redisLeaderKey, leaderTTL).Err(); err != nil {
			rc.logger.Error("Failed to refresh leader TTL",
				mlog.Err(err),
				mlog.String("node_id", rc.nodeID))
		}
	}
}

func (rc *redisCluster) heartbeatLoop() {
	defer rc.wg.Done()
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-rc.ctx.Done():
			return
		case <-ticker.C:
			rc.sendHeartbeat()
		}
	}
}

func (rc *redisCluster) sendHeartbeat() {
	ctx, cancel := context.WithTimeout(rc.ctx, 2*time.Second)
	defer cancel()

	nodeInfo := &model.ClusterInfo{
		Id:      rc.nodeID,
		Version: model.CurrentVersion,
	}

	data, err := json.Marshal(nodeInfo)
	if err != nil {
		rc.logger.Error("Failed to marshal node info",
			mlog.Err(err),
			mlog.String("node_id", rc.nodeID))
		return
	}

	// Use multi to ensure atomicity
	tx := rc.rdb.TxPipeline()

	// Set node info
	key := fmt.Sprintf("%s:%s", redisNodesKey, rc.nodeID)
	tx.Set(ctx, key, string(data), nodesTTL)

	// Set sync version
	tx.Set(ctx, fmt.Sprintf("%s:sync:%s", redisNodesKey, rc.nodeID), rc.syncVersion.Load(), nodesTTL)

	if _, err := tx.Exec(ctx); err != nil {
		rc.logger.Error("Failed to send heartbeat",
			mlog.Err(err),
			mlog.String("node_id", rc.nodeID))
	}
}

func (rc *redisCluster) messageListener() {
	defer rc.wg.Done()
	ch := rc.pubsub.Channel()

	for {
		select {
		case <-rc.ctx.Done():
			return
		case msg := <-ch:
			if msg == nil {
				continue
			}

			switch msg.Channel {
			case redisPubSubChannel:
				rc.handleClusterMessage(msg.Payload)
			case redisEventChannel:
				rc.handleEventMessage(msg.Payload)
			}
		}
	}
}

func (rc *redisCluster) messageProcessor() {
	defer rc.wg.Done()

	for {
		select {
		case <-rc.ctx.Done():
			return
		case msg := <-rc.messageBuffer:
			if !rc.isReady.Load() {
				// Skip processing if not ready
				continue
			}
			rc.processMessage(&msg)
		}
	}
}

func (rc *redisCluster) processMessage(msg *model.ClusterMessage) {
	// Handle WebSocket events
	if msg.Event == model.ClusterEventPublish {
		wsMsg, err := model.WebSocketEventFromJSON(bytes.NewReader(msg.Data))
		if err != nil {
			rc.logger.Error("Failed to deserialize WebSocket event",
				mlog.Err(err),
				mlog.String("node_id", rc.nodeID))
			return
		}
		rc.ps.PublishSkipClusterSend(wsMsg)
		return
	}

	rc.handlersMutex.RLock()
	handlers, ok := rc.handlers[msg.Event]
	rc.handlersMutex.RUnlock()

	if !ok {
		return
	}

	for _, handler := range handlers {
		handler(msg)
	}
}

func (rc *redisCluster) handleClusterMessage(payload string) {
	var msg model.ClusterMessage
	if err := json.Unmarshal([]byte(payload), &msg); err != nil {
		rc.logger.Error("Failed to unmarshal cluster message",
			mlog.Err(err),
			mlog.String("node_id", rc.nodeID))
		return
	}

	rc.logger.Debug("Received cluster message",
		mlog.String("event", string(msg.Event)),
		mlog.String("node_id", rc.nodeID),
		mlog.Bool("has_handler", rc.hasHandler(msg.Event)))

	select {
	case rc.messageBuffer <- msg:
		// Message buffered successfully
	default:
		rc.logger.Warn("Message buffer full, dropping message",
			mlog.String("event", string(msg.Event)),
			mlog.String("node_id", rc.nodeID))
	}
}

func (rc *redisCluster) handleEventMessage(payload string) {
	var ev model.PluginClusterEvent
	if err := json.Unmarshal([]byte(payload), &ev); err != nil {
		rc.logger.Error("Failed to unmarshal plugin event", mlog.Err(err))
		return
	}

	rc.handlersMutex.RLock()
	handlers, ok := rc.handlers[model.ClusterEventPluginEvent]
	rc.handlersMutex.RUnlock()

	if !ok {
		return
	}

	msg := &model.ClusterMessage{
		Event: model.ClusterEventPluginEvent,
		Props: map[string]string{
			"EventId": ev.Id,
		},
		Data: ev.Data,
	}

	for _, handler := range handlers {
		handler(msg)
	}
}

func (rc *redisCluster) hasHandler(event model.ClusterEvent) bool {
	rc.handlersMutex.RLock()
	defer rc.handlersMutex.RUnlock()
	_, ok := rc.handlers[event]
	return ok
}

// ClusterInterface implementation
func (rc *redisCluster) StartInterNodeCommunication() {
	if err := rc.Start(); err != nil {
		rc.logger.Error("Failed to start cluster communication", mlog.Err(err))
	}
}

func (rc *redisCluster) StopInterNodeCommunication() {
	rc.Stop()
}

func (rc *redisCluster) RegisterClusterMessageHandler(event model.ClusterEvent, handler einterfaces.ClusterMessageHandler) {
	rc.handlersMutex.Lock()
	defer rc.handlersMutex.Unlock()
	rc.handlers[event] = append(rc.handlers[event], handler)
}

func (rc *redisCluster) GetClusterId() string {
	return rc.nodeID
}

func (rc *redisCluster) IsLeader() bool {
	return rc.isLeader.Load()
}

func (rc *redisCluster) GetMyClusterInfo() *model.ClusterInfo {
	return &model.ClusterInfo{
		Id:      rc.nodeID,
		Version: model.CurrentVersion,
	}
}

func (rc *redisCluster) GetClusterInfos() []*model.ClusterInfo {
	ctx := context.Background()
	pattern := fmt.Sprintf("%s:*", redisNodesKey)
	keys, err := rc.rdb.Keys(ctx, pattern).Result()
	if err != nil {
		rc.logger.Error("Failed to get cluster nodes", mlog.Err(err))
		return nil
	}

	var infos []*model.ClusterInfo
	for _, key := range keys {
		data, err := rc.rdb.Get(ctx, key).Result()
		if err != nil {
			continue
		}

		var info model.ClusterInfo
		if err := json.Unmarshal([]byte(data), &info); err != nil {
			continue
		}
		infos = append(infos, &info)
	}
	return infos
}

func (rc *redisCluster) SendClusterMessage(msg *model.ClusterMessage) {
	data, err := json.Marshal(msg)
	if err != nil {
		rc.logger.Error("Failed to marshal cluster message",
			mlog.Err(err),
			mlog.String("node_id", rc.nodeID))
		return
	}

	channel := redisPubSubChannel
	if msg.Event == model.ClusterEventPluginEvent {
		channel = redisEventChannel
	}

	ctx, cancel := context.WithTimeout(rc.ctx, 2*time.Second)
	defer cancel()

	// Use multi to ensure atomicity
	tx := rc.rdb.TxPipeline()

	// Publish message
	tx.Publish(ctx, channel, string(data))

	// Update sync version
	rc.syncVersion.Add(1)
	tx.Set(ctx, fmt.Sprintf("%s:sync:%s", redisNodesKey, rc.nodeID), rc.syncVersion.Load(), nodesTTL)

	if _, err := tx.Exec(ctx); err != nil {
		rc.logger.Error("Failed to publish cluster message",
			mlog.Err(err),
			mlog.String("node_id", rc.nodeID))
	}
}

func (rc *redisCluster) SendClusterMessageToNode(nodeID string, msg *model.ClusterMessage) error {
	// In Redis implementation, all messages are broadcasted
	rc.SendClusterMessage(msg)
	return nil
}

func (rc *redisCluster) GetClusterStats(ctx request.CTX) ([]*model.ClusterStats, *model.AppError) {
	nodes := rc.GetClusterInfos()
	stats := make([]*model.ClusterStats, 0, len(nodes))

	for _, node := range nodes {
		stat := &model.ClusterStats{
			Id: node.Id,
		}
		stats = append(stats, stat)
	}

	return stats, nil
}

func (rc *redisCluster) GetLogs(ctx request.CTX, page, perPage int) ([]string, *model.AppError) {
	// Not implemented for Redis cluster
	return []string{}, nil
}

func (rc *redisCluster) GetPluginStatuses() (model.PluginStatuses, *model.AppError) {
	// Not implemented for Redis cluster
	return model.PluginStatuses{}, nil
}

func (rc *redisCluster) ConfigChanged(old, new *model.Config, sendToOtherServer bool) *model.AppError {
	// Handle config changes if needed
	return nil
}

func (rc *redisCluster) GenerateSupportPacket(ctx request.CTX, opts *model.SupportPacketOptions) (map[string][]model.FileData, error) {
	result := make(map[string][]model.FileData)

	// Add cluster information
	clusterInfo := &model.ClusterInfo{
		Id:      rc.nodeID,
		Version: model.CurrentVersion,
	}

	infoBytes, err := json.Marshal(clusterInfo)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal cluster info: %w", err)
	}

	result["cluster_info"] = []model.FileData{{
		Filename: "cluster_info.json",
		Body:     infoBytes,
	}}

	// Add node status
	nodes := rc.GetClusterInfos()
	nodesBytes, err := json.Marshal(nodes)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal nodes info: %w", err)
	}

	result["cluster_nodes"] = []model.FileData{{
		Filename: "cluster_nodes.json",
		Body:     nodesBytes,
	}}

	return result, nil
}

func (rc *redisCluster) GetWSQueues(userID, connectionID string, seqNum int64) (map[string]*model.WSQueues, error) {
	// For Redis implementation, we don't track WebSocket queues per node
	// Return empty map as we handle WebSocket events through Redis pub/sub
	return map[string]*model.WSQueues{}, nil
}

func (rc *redisCluster) QueryLogs(rctx request.CTX, page, perPage int) (map[string][]string, *model.AppError) {
	// Not implemented for Redis cluster
	return map[string][]string{}, nil
}

func (rc *redisCluster) WebConnCountForUser(userID string) (int, *model.AppError) {
	// For Redis implementation, we don't track connection counts per node
	return 0, nil
}

func (rc *redisCluster) NotifyMsg(buf []byte) {
	// For Redis implementation, we use pub/sub instead
}

func (rc *redisCluster) HealthScore() int {
	ctx, cancel := context.WithTimeout(rc.ctx, 2*time.Second)
	defer cancel()

	// Check Redis connection
	if err := rc.rdb.Ping(ctx).Err(); err != nil {
		return 100 // Unhealthy
	}

	// Check if we can publish/subscribe
	if rc.pubsub == nil {
		return 100 // Unhealthy
	}

	return 0 // Healthy
}

func (rc *redisCluster) SetReady() {
	rc.isReady.Store(true)
	rc.logger.Info("Redis cluster node is ready", mlog.String("node_id", rc.nodeID))
}

func (rc *redisCluster) syncLoop() {
	defer rc.wg.Done()
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-rc.ctx.Done():
			return
		case <-ticker.C:
			rc.verifyClusterSync()
		}
	}
}

func (rc *redisCluster) verifyClusterSync() {
	// Get all cluster nodes
	nodes := rc.GetClusterInfos()
	if len(nodes) == 0 {
		rc.logger.Warn("No cluster nodes found during sync verification",
			mlog.String("node_id", rc.nodeID))
		return
	}

	// Check version consistency
	for _, node := range nodes {
		if node.Version != model.CurrentVersion {
			rc.logger.Error("Version mismatch detected",
				mlog.String("node_id", rc.nodeID),
				mlog.String("remote_node_id", node.Id),
				mlog.String("local_version", model.CurrentVersion),
				mlog.String("remote_version", node.Version))
		}
	}

	// Update sync timestamp
	rc.lastSync.Store(time.Now().UnixNano())
}
