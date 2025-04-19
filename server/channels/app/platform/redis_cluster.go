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
	redisPubSubChannel    = "mattermost_cluster"
	redisLeaderKey        = "mattermost_cluster_leader"
	redisNodesKey         = "mattermost_cluster_nodes"
	redisEventChannel     = "mattermost_cluster_events"
	redisReliablePrefix   = "mattermost_cluster_reliable"
	leaderTTL             = 15 * time.Second
	nodesTTL              = 20 * time.Second
	heartbeatInterval     = 5 * time.Second
	maxMessageBuffer      = 1000
	syncTimeout           = 10 * time.Second
	reliableMessageTTL    = 24 * time.Hour
	reliableProcessDelay  = 500 * time.Millisecond
	reliableRetryInterval = 5 * time.Second
)

// Improved Redis Cluster implementation
type redisCluster struct {
	ps              *PlatformService
	nodeID          string
	rdb             *redis.Client
	pubsub          *redis.PubSub
	handlers        map[model.ClusterEvent][]einterfaces.ClusterMessageHandler
	handlersMutex   sync.RWMutex
	isLeader        atomic.Bool
	leaderMutex     sync.RWMutex
	stopChan        chan struct{}
	wg              sync.WaitGroup
	logger          *mlog.Logger
	isReady         atomic.Bool
	messageBuffer   chan model.ClusterMessage
	ctx             context.Context
	cancel          context.CancelFunc
	lastSync        atomic.Int64
	syncVersion     atomic.Int64
	broadcastHooks  map[string]BroadcastHook
	processedMsgs   sync.Map
	retryQueue      chan *model.ClusterMessage
	retryMutex      sync.Mutex
	reliableEnabled bool
}

// NewRedisCluster creates a new Redis-based cluster implementation
func NewRedisCluster(ps *PlatformService, hooks map[string]BroadcastHook) *redisCluster {
	nodeID := model.NewId()
	ctx, cancel := context.WithCancel(context.Background())

	rc := &redisCluster{
		ps:              ps,
		nodeID:          nodeID,
		handlers:        make(map[model.ClusterEvent][]einterfaces.ClusterMessageHandler),
		stopChan:        make(chan struct{}),
		logger:          ps.logger.With(mlog.String("cluster_node_id", nodeID)),
		messageBuffer:   make(chan model.ClusterMessage, maxMessageBuffer),
		ctx:             ctx,
		cancel:          cancel,
		broadcastHooks:  hooks,
		retryQueue:      make(chan *model.ClusterMessage, 1000),
		reliableEnabled: true,
	}

	cfg := ps.Config().CacheSettings
	rc.rdb = redis.NewClient(&redis.Options{
		Addr:     *cfg.RedisAddress,
		Password: *cfg.RedisPassword,
		DB:       int(*cfg.RedisDB),
	})

	// Register the platform's cluster handlers
	rc.RegisterClusterMessageHandler(model.ClusterEventPublish, ps.ClusterPublishHandler)
	rc.RegisterClusterMessageHandler(model.ClusterEventUpdateStatus, ps.ClusterUpdateStatusHandler)
	rc.RegisterClusterMessageHandler(model.ClusterEventInvalidateAllCaches, ps.ClusterInvalidateAllCachesHandler)
	rc.RegisterClusterMessageHandler(model.ClusterEventInvalidateWebConnCacheForUser, ps.clusterInvalidateWebConnSessionCacheForUserHandler)
	rc.RegisterClusterMessageHandler(model.ClusterEventBusyStateChanged, ps.clusterBusyStateChgHandler)

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
	rc.wg.Add(7) // Added retry processor
	go rc.leaderElectionLoop()
	go rc.heartbeatLoop()
	go rc.messageListener()
	go rc.messageProcessor()
	go rc.syncLoop()
	go rc.processReliableMessages()
	go rc.processRetryQueue()

	// Mark the node as ready
	rc.isReady.Store(true)

	rc.logger.Info("Redis cluster started",
		mlog.String("node_id", rc.nodeID),
		mlog.String("redis_address", *rc.ps.Config().CacheSettings.RedisAddress),
		mlog.Bool("reliable_enabled", rc.reliableEnabled))

	return nil
}

func (rc *redisCluster) Stop() {
	rc.logger.Info("Stopping Redis cluster", mlog.String("node_id", rc.nodeID))
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

	// Try to become leader using SET NX (only set if not exists)
	success, err := rc.rdb.SetNX(ctx, redisLeaderKey, rc.nodeID, leaderTTL).Result()

	if err != nil {
		rc.logger.Error("Failed to perform leader election",
			mlog.Err(err),
			mlog.String("node_id", rc.nodeID))
		return
	}

	// If not successful, verify who is the leader
	if !success {
		// Check who is the current leader
		leaderID, err := rc.rdb.Get(ctx, redisLeaderKey).Result()
		if err != nil {
			if err != redis.Nil {
				rc.logger.Error("Failed to get current leader",
					mlog.Err(err),
					mlog.String("node_id", rc.nodeID))
			}
			// Consider it's not the leader
			rc.isLeader.Store(false)
		} else {
			// Still the leader if the ID matches
			rc.isLeader.Store(leaderID == rc.nodeID)
		}
	} else {
		// Successfully became the leader
		rc.isLeader.Store(true)
	}

	isLeader := rc.isLeader.Load()

	// Set sync version for this node
	_, err = rc.rdb.Set(ctx, fmt.Sprintf("%s:sync:%s", redisNodesKey, rc.nodeID), rc.syncVersion.Load(), nodesTTL).Result()
	if err != nil {
		rc.logger.Error("Failed to set sync version",
			mlog.Err(err),
			mlog.String("node_id", rc.nodeID))
	}

	// Check if leader status changed
	if wasLeader != isLeader {
		if isLeader {
			rc.logger.Info("Became cluster leader", mlog.String("node_id", rc.nodeID))
			// Trigger immediate sync when becoming leader
			go rc.verifyClusterSync()
		} else {
			rc.logger.Info("Lost cluster leadership", mlog.String("node_id", rc.nodeID))
		}
		// Notify platform service
		rc.ps.InvokeClusterLeaderChangedListeners()
	}

	// If leader, refresh TTL to maintain leadership
	if isLeader {
		_, err := rc.rdb.Expire(ctx, redisLeaderKey, leaderTTL).Result()
		if err != nil {
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

	// Use pipeline for multiple operations
	pipe := rc.rdb.Pipeline()

	// Set node info with TTL
	nodeKey := fmt.Sprintf("%s:%s", redisNodesKey, rc.nodeID)
	pipe.Set(ctx, nodeKey, string(data), nodesTTL)

	// Set sync version with TTL
	syncKey := fmt.Sprintf("%s:sync:%s", redisNodesKey, rc.nodeID)
	pipe.Set(ctx, syncKey, rc.syncVersion.Load(), nodesTTL)

	_, err = pipe.Exec(ctx)
	if err != nil {
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
				rc.logger.Debug("Node not ready, skipping message",
					mlog.String("event", string(msg.Event)),
					mlog.String("node_id", rc.nodeID))
				continue
			}
			rc.processMessage(&msg)
		}
	}
}

func (rc *redisCluster) processMessage(msg *model.ClusterMessage) {
	// Skip if this message came from this node
	if msg.Props != nil && msg.Props["source_node_id"] == rc.nodeID {
		return
	}

	// Check for duplicates
	msgID := msg.Props["msg_id"]
	if msgID != "" {
		if _, exists := rc.processedMsgs.LoadOrStore(msgID, true); exists {
			return
		}
		// Cleanup after processing to avoid memory leak
		defer func() {
			time.AfterFunc(5*time.Minute, func() {
				rc.processedMsgs.Delete(msgID)
			})
		}()
	}

	rc.logger.Debug("Processing message",
		mlog.String("event", string(msg.Event)),
		mlog.String("node_id", rc.nodeID),
		mlog.String("msg_id", msgID))

	// Handle WebSocket events
	if msg.Event == model.ClusterEventPublish {
		wsMsg, err := model.WebSocketEventFromJSON(bytes.NewReader(msg.Data))
		if err != nil {
			rc.logger.Error("Failed to deserialize WebSocket event",
				mlog.Err(err),
				mlog.String("node_id", rc.nodeID))
			return
		}
		// Set from cluster to avoid re-sending
		wsMsg.SetFromCluster(true)
		rc.ps.PublishSkipClusterSend(wsMsg)
		return
	}

	// Get handlers for this event
	rc.handlersMutex.RLock()
	handlers, ok := rc.handlers[msg.Event]
	rc.handlersMutex.RUnlock()

	if !ok {
		return
	}

	// Call all registered handlers
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

	// Ensure Props is initialized
	if msg.Props == nil {
		msg.Props = make(map[string]string)
	}

	rc.logger.Debug("Received cluster message",
		mlog.String("event", string(msg.Event)),
		mlog.String("node_id", rc.nodeID),
		mlog.Bool("has_handler", rc.hasHandler(msg.Event)))

	// Add to message buffer
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
			"EventId":        ev.Id,
			"source_node_id": rc.nodeID,
			"msg_id":         model.NewId(),
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
	ctx, cancel := context.WithTimeout(rc.ctx, 5*time.Second)
	defer cancel()

	pattern := fmt.Sprintf("%s:*", redisNodesKey)
	keys, err := rc.rdb.Keys(ctx, pattern).Result()
	if err != nil {
		rc.logger.Error("Failed to get cluster nodes", mlog.Err(err))
		return nil
	}

	var infos []*model.ClusterInfo
	for _, key := range keys {
		// Skip sync keys
		if bytes.Contains([]byte(key), []byte(":sync:")) {
			continue
		}

		data, err := rc.rdb.Get(ctx, key).Result()
		if err != nil {
			rc.logger.Debug("Failed to get node info",
				mlog.String("key", key),
				mlog.Err(err))
			continue
		}

		var info model.ClusterInfo
		if err := json.Unmarshal([]byte(data), &info); err != nil {
			rc.logger.Debug("Failed to unmarshal node info",
				mlog.String("key", key),
				mlog.Err(err))
			continue
		}
		infos = append(infos, &info)
	}

	rc.logger.Debug("Got cluster information",
		mlog.Int("node_count", len(infos)),
		mlog.String("node_id", rc.nodeID))

	return infos
}

func (rc *redisCluster) SendClusterMessage(msg *model.ClusterMessage) {
	// Initialize props if needed
	if msg.Props == nil {
		msg.Props = make(map[string]string)
	}

	// Add source node ID and message ID for tracking
	msg.Props["source_node_id"] = rc.nodeID
	msg.Props["msg_id"] = model.NewId()

	data, err := json.Marshal(msg)
	if err != nil {
		rc.logger.Error("Failed to marshal cluster message",
			mlog.Err(err),
			mlog.String("node_id", rc.nodeID))
		return
	}

	ctx, cancel := context.WithTimeout(rc.ctx, 2*time.Second)
	defer cancel()

	// Use specific channel for plugin events
	channel := redisPubSubChannel
	if msg.Event == model.ClusterEventPluginEvent {
		channel = redisEventChannel
	}

	// Handle reliable message delivery
	if rc.reliableEnabled && msg.SendType == model.ClusterSendReliable {
		key := fmt.Sprintf("%s:%s:%s", redisReliablePrefix, string(msg.Event), msg.Props["msg_id"])

		// Store message in Redis for reliable delivery
		if err := rc.rdb.Set(ctx, key, string(data), reliableMessageTTL).Err(); err != nil {
			rc.logger.Error("Failed to store reliable message",
				mlog.Err(err),
				mlog.String("node_id", rc.nodeID),
				mlog.String("msg_id", msg.Props["msg_id"]))
		}

		// Add to list of reliable messages to process
		listKey := fmt.Sprintf("%s:list", redisReliablePrefix)
		if err := rc.rdb.RPush(ctx, listKey, key).Err(); err != nil {
			rc.logger.Error("Failed to add to reliable message list",
				mlog.Err(err),
				mlog.String("node_id", rc.nodeID))
		}

		// Ensure list has TTL
		rc.rdb.Expire(ctx, listKey, reliableMessageTTL)
	}

	// Use pipeline for multiple operations
	pipe := rc.rdb.Pipeline()

	// Publish message
	pipe.Publish(ctx, channel, string(data))

	// Update sync version
	syncVer := rc.syncVersion.Add(1)
	pipe.Set(ctx, fmt.Sprintf("%s:sync:%s", redisNodesKey, rc.nodeID), syncVer, nodesTTL)

	_, err = pipe.Exec(ctx)
	if err != nil {
		rc.logger.Error("Failed to publish cluster message",
			mlog.Err(err),
			mlog.String("event", string(msg.Event)),
			mlog.String("node_id", rc.nodeID))

		// Add to retry queue
		if msg.SendType == model.ClusterSendReliable {
			select {
			case rc.retryQueue <- msg:
				rc.logger.Debug("Added message to retry queue",
					mlog.String("event", string(msg.Event)),
					mlog.String("msg_id", msg.Props["msg_id"]))
			default:
				rc.logger.Error("Retry queue full, dropping message",
					mlog.String("event", string(msg.Event)))
			}
		}
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
	// Nothing to do for config changes
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
	// Unused in Redis implementation
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

// processReliableMessages processes messages that were sent reliably
func (rc *redisCluster) processReliableMessages() {
	defer rc.wg.Done()
	ticker := time.NewTicker(reliableProcessDelay)
	defer ticker.Stop()

	for {
		select {
		case <-rc.ctx.Done():
			return
		case <-ticker.C:
			if !rc.isReady.Load() {
				continue
			}

			ctx, cancel := context.WithTimeout(rc.ctx, 5*time.Second)

			// Get the list of reliable messages
			listKey := fmt.Sprintf("%s:list", redisReliablePrefix)

			// Get up to 100 messages at a time
			keys, err := rc.rdb.LRange(ctx, listKey, 0, 99).Result()
			if err != nil {
				rc.logger.Error("Failed to get reliable message list",
					mlog.Err(err),
					mlog.String("node_id", rc.nodeID))
				cancel()
				continue
			}

			// Process each message
			for _, key := range keys {
				// Get the message
				data, err := rc.rdb.Get(ctx, key).Result()
				if err != nil {
					if err != redis.Nil {
						rc.logger.Error("Failed to get reliable message",
							mlog.Err(err),
							mlog.String("key", key))
					}

					// Remove from list regardless since it doesn't exist
					rc.rdb.LRem(ctx, listKey, 1, key)
					continue
				}

				// Parse the message
				var msg model.ClusterMessage
				if err := json.Unmarshal([]byte(data), &msg); err != nil {
					rc.logger.Error("Failed to unmarshal reliable message",
						mlog.Err(err),
						mlog.String("key", key))

					// Remove invalid message
					rc.rdb.LRem(ctx, listKey, 1, key)
					continue
				}

				// Process the message
				rc.logger.Debug("Processing reliable message",
					mlog.String("event", string(msg.Event)),
					mlog.String("msg_id", msg.Props["msg_id"]))

				// Add to message buffer
				select {
				case rc.messageBuffer <- msg:
					// Successfully buffered
				default:
					rc.logger.Warn("Message buffer full, will retry reliable message later",
						mlog.String("event", string(msg.Event)))
					// Continue without removing from the list
					continue
				}

				// Remove from list after processing
				rc.rdb.LRem(ctx, listKey, 1, key)
			}

			cancel()
		}
	}
}

// processRetryQueue processes messages that failed to be sent
func (rc *redisCluster) processRetryQueue() {
	defer rc.wg.Done()
	ticker := time.NewTicker(reliableRetryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-rc.ctx.Done():
			return
		case <-ticker.C:
			// Process retry queue if ready
			if !rc.isReady.Load() {
				continue
			}

		retryLoop:
			for {
				select {
				case msg := <-rc.retryQueue:
					rc.logger.Debug("Retrying message from queue",
						mlog.String("event", string(msg.Event)),
						mlog.String("msg_id", msg.Props["msg_id"]))

					data, err := json.Marshal(msg)
					if err != nil {
						rc.logger.Error("Failed to marshal retry message",
							mlog.Err(err),
							mlog.String("event", string(msg.Event)))
						continue
					}

					ctx, cancel := context.WithTimeout(rc.ctx, 2*time.Second)

					// Determine channel based on event type
					channel := redisPubSubChannel
					if msg.Event == model.ClusterEventPluginEvent {
						channel = redisEventChannel
					}

					// Publish the message
					err = rc.rdb.Publish(ctx, channel, string(data)).Err()
					if err != nil {
						rc.logger.Error("Failed to publish retry message",
							mlog.Err(err),
							mlog.String("event", string(msg.Event)))

						// Put back in queue if it's still reliable
						if msg.SendType == model.ClusterSendReliable {
							select {
							case rc.retryQueue <- msg:
								// Successfully re-queued
							default:
								rc.logger.Error("Retry queue full, dropping message",
									mlog.String("event", string(msg.Event)))
							}
						}
					}

					cancel()
				default:
					// No more messages in queue
					break retryLoop
				}
			}
		}
	}
}

// BroadcastWebSocketEvent broadcasts WebSocket events to all nodes
func (rc *redisCluster) BroadcastWebSocketEvent(event *model.WebSocketEvent) {
	// Skip if the event is already from another cluster node
	if event.IsFromCluster() {
		return
	}

	// Apply broadcast hooks if defined
	if len(event.GetBroadcast().BroadcastHooks) > 0 {
		ev, hooks, hookArgs := event.WithoutBroadcastHooks()

		for i, hookID := range hooks {
			if hook, ok := rc.broadcastHooks[hookID]; ok {
				var args map[string]any
				if i < len(hookArgs) {
					args = hookArgs[i]
				}

				// Create a HookedWebSocketEvent to pass to the hook
				hookedEvent := MakeHookedWebSocketEvent(ev)

				// Call the Process method instead of calling hook directly
				if err := hook.Process(hookedEvent, nil, args); err != nil {
					rc.logger.Error("Failed to process broadcast hook",
						mlog.String("hook_id", hookID),
						mlog.Err(err))
					continue
				}

				// Get the processed event
				ev = hookedEvent.Event()
			}
		}

		event = ev
	}

	// Mark the event as coming from cluster
	event = event.SetFromCluster(true)

	// Convert WebSocketEvent to JSON bytes
	data, err := event.ToJSON()
	if err != nil {
		rc.logger.Error("Failed to marshal WebSocket event", mlog.Err(err))
		return
	}

	// Create cluster message
	msg := &model.ClusterMessage{
		Event:    model.ClusterEventPublish,
		Data:     data,
		SendType: model.ClusterSendBestEffort,
	}

	// Determine if this should be sent reliably based on event type
	needsReliableDelivery := false

	switch event.EventType() {
	case model.WebsocketEventPosted,
		model.WebsocketEventPostEdited,
		model.WebsocketEventPostDeleted,
		model.WebsocketEventDirectAdded,
		model.WebsocketEventGroupAdded,
		model.WebsocketEventAddedToTeam,
		model.WebsocketEventLeaveTeam,
		model.WebsocketEventUpdateTeam,
		model.WebsocketEventUserAdded,
		model.WebsocketEventUserUpdated,
		model.WebsocketEventStatusChange,
		model.WebsocketEventHello,
		model.WebsocketEventChannelUpdated,
		model.WebsocketEventChannelCreated,
		model.WebsocketEventChannelDeleted:
		needsReliableDelivery = true
	}

	// Override reliable flag from broadcast
	if event.GetBroadcast() != nil && event.GetBroadcast().ReliableClusterSend {
		needsReliableDelivery = true
	}

	if needsReliableDelivery {
		msg.SendType = model.ClusterSendReliable
	}

	// Send to other nodes via Redis
	rc.SendClusterMessage(msg)
}

// Publish sends a WebSocket event to all nodes
func (rc *redisCluster) Publish(event *model.WebSocketEvent) {
	// Skip if the event is already from another cluster node
	if event.IsFromCluster() {
		return
	}

	// Mark the event as coming from cluster
	event = event.SetFromCluster(true)

	// Set the node ID in the broadcast if it exists
	if event.GetBroadcast() != nil {
		event = event.SetBroadcast(event.GetBroadcast())
		event.GetBroadcast().ConnectionId = rc.nodeID
	}

	// Local broadcast first for this node
	rc.ps.PublishSkipClusterSend(event)

	// Prepare cluster message for other nodes
	data, err := event.ToJSON()
	if err != nil {
		rc.logger.Error("Failed to marshal WebSocket event", mlog.Err(err))
		return
	}

	// Create a cluster message to send to other nodes
	msg := &model.ClusterMessage{
		Event:    model.ClusterEventPublish,
		Data:     data,
		SendType: model.ClusterSendBestEffort,
	}

	// Check if this needs reliable delivery
	if event.EventType() == model.WebsocketEventPosted ||
		event.EventType() == model.WebsocketEventPostEdited ||
		event.GetBroadcast().ReliableClusterSend {
		msg.SendType = model.ClusterSendReliable
	}

	// Send to all other nodes
	rc.SendClusterMessage(msg)
}
