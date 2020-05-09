/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hdds.scm.server.ratis;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigType;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.ha.SCMNodeDetails;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.netty.NettyConfigKeys;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.proto.RaftProtos.RoleInfoProto;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.GroupInfoReply;
import org.apache.ratis.protocol.GroupInfoRequest;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.rpc.RpcType;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.LifeCycle;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.hadoop.hdds.conf.ConfigTag.OZONE;
import static org.apache.hadoop.hdds.conf.ConfigTag.RATIS;
import static org.apache.hadoop.hdds.conf.ConfigTag.SCM;
import static org.apache.hadoop.hdds.conf.ConfigTag.HA;

/**
 * Class for SCM Ratis Server.
 */
public final class SCMRatisServer {
  private static final Logger LOG = LoggerFactory
      .getLogger(SCMRatisServer.class);

  private final StorageContainerManager scm;
  private final SCMStateMachine scmStateMachine;

  private final String storageDir;
  private final int port;
  private final InetSocketAddress scmRatisAddress;
  private final RaftServer server;
  private final RaftGroupId raftGroupId;
  private final RaftGroup raftGroup;
  private final RaftPeerId raftPeerId;

  private final ClientId clientId = ClientId.randomId();
  private final ScheduledExecutorService scheduledRoleChecker;
  private long roleCheckInitialDelayMs = 1000; // 1 second default
  private long roleCheckIntervalMs;
  private ReentrantReadWriteLock roleCheckLock = new ReentrantReadWriteLock();
  private Optional<RaftPeerRole> cachedPeerRole = Optional.empty();
  private Optional<RaftPeerId> cachedLeaderPeerId = Optional.empty();

  private static final AtomicLong CALL_ID_COUNTER = new AtomicLong();
  private static long nextCallId() {
    return CALL_ID_COUNTER.getAndIncrement() & Long.MAX_VALUE;
  }

  /**
   * Creates a SCM Ratis Server.
   * @throws IOException
   */
  private SCMRatisServer(SCMRatisServerConfiguration conf,
                         StorageContainerManager scm, String ratisStorageDir,
                         String raftGroupIdStr, RaftPeerId localRaftPeerId,
                         InetSocketAddress addr, List<RaftPeer> raftPeers)
      throws IOException {
    this.scm = scm;
    this.scmRatisAddress = addr;
    this.port = addr.getPort();
    this.storageDir = ratisStorageDir;
    RaftProperties serverProperties = newRaftProperties(conf);

    this.raftPeerId = localRaftPeerId;
    this.raftGroupId = RaftGroupId.valueOf(
        getRaftGroupIdFromOmServiceId(raftGroupIdStr));
    this.raftGroup = RaftGroup.valueOf(raftGroupId, raftPeers);

    StringBuilder raftPeersStr = new StringBuilder();
    for (RaftPeer peer : raftPeers) {
      raftPeersStr.append(", ").append(peer.getAddress());
    }
    LOG.info("Instantiating SCM Ratis server with GroupID: {} and " +
        "Raft Peers: {}", raftGroupIdStr, raftPeersStr.toString().substring(2));
    this.scmStateMachine = getStateMachine();

    this.server = RaftServer.newBuilder()
        .setServerId(this.raftPeerId)
        .setGroup(this.raftGroup)
        .setProperties(serverProperties)
        .setStateMachine(scmStateMachine)
        .build();

    // Run a scheduler to check and update the server role on the leader
    // periodically
    this.scheduledRoleChecker = Executors.newSingleThreadScheduledExecutor();
    this.scheduledRoleChecker.scheduleWithFixedDelay(new Runnable() {
      @Override
      public void run() {
        // Run this check only on the leader OM
        if (cachedPeerRole.isPresent() &&
            cachedPeerRole.get() == RaftPeerRole.LEADER) {
          updateServerRole();
        }
      }
    }, roleCheckInitialDelayMs, roleCheckIntervalMs, TimeUnit.MILLISECONDS);
  }

  /**
   * Create a SCM Ratis Server instance.
   */
  public static SCMRatisServer newSCMRatisServer(
      SCMRatisServerConfiguration conf, StorageContainerManager scm,
      SCMNodeDetails scmNodeDetails, List<SCMNodeDetails> peers,
      String ratisStorageDir)
      throws IOException {
    String scmServiceId = scmNodeDetails.getSCMServiceId();

    String scmNodeId = scmNodeDetails.getSCMNodeId();
    RaftPeerId localRaftPeerId = RaftPeerId.getRaftPeerId(scmNodeId);
    InetSocketAddress ratisAddr = new InetSocketAddress(
        scmNodeDetails.getAddress(), scmNodeDetails.getRatisPort());

    RaftPeer localRaftPeer = new RaftPeer(localRaftPeerId, ratisAddr);

    List<RaftPeer> raftPeers = new ArrayList<>();
    raftPeers.add(localRaftPeer);

    for (SCMNodeDetails peer : peers) {
      String peerNodeId = peer.getSCMNodeId();
      InetSocketAddress peerRatisAddr = new InetSocketAddress(
          peer.getAddress(), peer.getRatisPort());
      RaftPeerId raftPeerId = RaftPeerId.valueOf(peerNodeId);
      RaftPeer raftPeer = new RaftPeer(raftPeerId, peerRatisAddr);
      // Add other SCMs in Ratis ring
      raftPeers.add(raftPeer);
    }

    return new SCMRatisServer(conf, scm, ratisStorageDir, scmServiceId,
        localRaftPeerId, ratisAddr, raftPeers);
  }

  private UUID getRaftGroupIdFromOmServiceId(String scmServiceId) {
    return UUID.nameUUIDFromBytes(scmServiceId.getBytes(
        StandardCharsets.UTF_8));
  }

  private SCMStateMachine getStateMachine() {
    return new SCMStateMachine(this);
  }

  private RaftProperties newRaftProperties(SCMRatisServerConfiguration conf) {
    final RaftProperties properties = new RaftProperties();
    // Set RPC type
    final RpcType rpc = SupportedRpcType.valueOfIgnoreCase(conf.getRpcType());
    RaftConfigKeys.Rpc.setType(properties, rpc);
    // Set the ratis port number
    if (rpc == SupportedRpcType.GRPC) {
      GrpcConfigKeys.Server.setPort(properties, port);
    } else if (rpc == SupportedRpcType.NETTY) {
      NettyConfigKeys.Server.setPort(properties, port);
    }
    // Set Ratis storage directory
    RaftServerConfigKeys.setStorageDir(properties,
        Collections.singletonList(new File(storageDir)));
    // Set RAFT segment size
    RaftServerConfigKeys.Log.setSegmentSizeMax(properties,
        SizeInBytes.valueOf((long)conf.getSegmentSize()));
    // Set RAFT segment pre-allocated size
    RaftServerConfigKeys.Log.Appender.setBufferElementLimit(properties,
        (int)conf.getLogAppenderQueueByteLimit());
    RaftServerConfigKeys.Log.Appender.setBufferByteLimit(properties,
        SizeInBytes.valueOf(conf.getLogAppenderQueueNum()));
    RaftServerConfigKeys.Log.setPreallocatedSize(properties,
        SizeInBytes.valueOf((int)conf.getPreallocatedSize()));
    RaftServerConfigKeys.Log.Appender.setInstallSnapshotEnabled(properties,
        false);
    RaftServerConfigKeys.Log.setPurgeGap(properties, conf.getLogPurgeGap());
    // For grpc set the maximum message size
    // TODO: calculate the optimal max message size
    GrpcConfigKeys.setMessageSizeMax(properties,
        SizeInBytes.valueOf((int)conf.getLogAppenderQueueByteLimit()));

    // Set the server request timeout
    final TimeDuration serverRequestTimeout = TimeDuration.valueOf(
        conf.getRequestTimeout(), TimeUnit.MILLISECONDS);
    RaftServerConfigKeys.Rpc.setRequestTimeout(properties,
        serverRequestTimeout);
    // Set timeout for server retry cache entry
    final TimeDuration retryCacheTimeout = TimeDuration.valueOf(
        conf.getRetryCacheTimeout(), TimeUnit.MILLISECONDS);
    RaftServerConfigKeys.RetryCache.setExpiryTime(properties,
        retryCacheTimeout);
    // Set the server min and max timeout
    final TimeDuration serverMinTimeout = TimeDuration.valueOf(
        conf.getMinTimeout(), TimeUnit.MILLISECONDS);
    final TimeDuration serverMaxTimeout = TimeDuration.valueOf(
        conf.getMinTimeout() + 200L, TimeUnit.MILLISECONDS);
    RaftServerConfigKeys.Rpc.setTimeoutMin(properties,
        serverMinTimeout);
    RaftServerConfigKeys.Rpc.setTimeoutMax(properties,
        serverMaxTimeout);
    // Set the number of maximum cached segments
    RaftServerConfigKeys.Log.setSegmentCacheNumMax(properties, 2);
    // TODO: set max write buffer size
    // Set the ratis leader election timeout
    final TimeDuration leaderElectionMinTimeout = TimeDuration.valueOf(
        conf.getMinLeaderElectionTimeout(), TimeUnit.MILLISECONDS);
    RaftServerConfigKeys.Rpc.setTimeoutMin(properties,
        leaderElectionMinTimeout);
    long leaderElectionMaxTimeout = leaderElectionMinTimeout.toLong(
        TimeUnit.MILLISECONDS) + 200;
    RaftServerConfigKeys.Rpc.setTimeoutMax(properties,
        TimeDuration.valueOf(leaderElectionMaxTimeout, TimeUnit.MILLISECONDS));

    final TimeDuration nodeFailureTimeout = TimeDuration.valueOf(
        conf.getFailureTimeout(), TimeUnit.MILLISECONDS);
    RaftServerConfigKeys.Notification.setNoLeaderTimeout(properties,
        nodeFailureTimeout);
    RaftServerConfigKeys.Rpc.setSlownessTimeout(properties,
        nodeFailureTimeout);

    // Ratis leader role check
    this.roleCheckIntervalMs = conf.getRoleCheckerInterval();
    this.roleCheckInitialDelayMs = leaderElectionMinTimeout
        .toLong(TimeUnit.MILLISECONDS);

    return properties;
  }

  /**
   * Start the Ratis server.
   * @throws IOException
   */
  public void start() throws IOException {
    LOG.info("Starting {} {} at port {}", getClass().getSimpleName(),
        server.getId(), port);
    server.start();
  }

  /**
   * Stop the Ratis server.
   */
  public void stop() {
    try {
      server.close();
      scmStateMachine.stop();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private boolean checkCachedPeerRoleIsLeader() {
    this.roleCheckLock.readLock().lock();
    try {
      if (cachedPeerRole.isPresent() &&
          cachedPeerRole.get() ==RaftPeerRole.LEADER) {
        return true;
      }
      return false;
    } finally {
      this.roleCheckLock.readLock().unlock();
    }
  }

  public boolean isLeader() {
    if (checkCachedPeerRoleIsLeader()) {
      return true;
    }

    // Get the server role from ratis server and update the cached values.
    updateServerRole();

    // After updating the server role, check and return if leader or not.
    return checkCachedPeerRoleIsLeader();
  }

  @VisibleForTesting
  public LifeCycle.State getServerState() {
    return server.getLifeCycleState();
  }

  @VisibleForTesting
  public RaftPeerId getRaftPeerId() {
    return this.raftPeerId;
  }

  public RaftGroup getRaftGroup() {
    return this.raftGroup;
  }

  /**
   * Get the local directory where ratis logs will be stored.
   */
  public static String getSCMRatisDirectory(ConfigurationSource conf) {
    String storageDir = conf.get(ScmConfigKeys.OZONE_SCM_RATIS_STORAGE_DIR);

    if (Strings.isNullOrEmpty(storageDir)) {
      storageDir = ServerUtils.getDefaultRatisDirectory(conf);
    }
    return storageDir;
  }

  public Optional<RaftPeerId> getCachedLeaderPeerId() {
    this.roleCheckLock.readLock().lock();
    try {
      return cachedLeaderPeerId;
    } finally {
      this.roleCheckLock.readLock().unlock();
    }
  }

  public StorageContainerManager getSCM() {
    return scm;
  }

  @VisibleForTesting
  public SCMStateMachine getScmStateMachine() {
    return scmStateMachine;
  }

  public int getServerPort() {
    return port;
  }

  public void updateServerRole() {
    try {
      GroupInfoReply groupInfo = getGroupInfo();
      RoleInfoProto roleInfoProto = groupInfo.getRoleInfoProto();
      RaftPeerRole thisNodeRole = roleInfoProto.getRole();

      if (thisNodeRole.equals(RaftPeerRole.LEADER)) {
        setServerRole(thisNodeRole, raftPeerId);

      } else if (thisNodeRole.equals(RaftPeerRole.FOLLOWER)) {
        ByteString leaderNodeId = roleInfoProto.getFollowerInfo()
            .getLeaderInfo().getId().getId();
        // There may be a chance, here we get leaderNodeId as null. For
        // example, in 3 node OM Ratis, if 2 OM nodes are down, there will
        // be no leader.
        RaftPeerId leaderPeerId = null;
        if (leaderNodeId != null && !leaderNodeId.isEmpty()) {
          leaderPeerId = RaftPeerId.valueOf(leaderNodeId);
        }

        setServerRole(thisNodeRole, leaderPeerId);

      } else {
        setServerRole(thisNodeRole, null);

      }
    } catch (IOException e) {
      LOG.error("Failed to retrieve RaftPeerRole. Setting cached role to " +
          "{} and resetting leader info.", RaftPeerRole.UNRECOGNIZED, e);
      setServerRole(null, null);
    }
  }

  public TermIndex getLastAppliedTermIndex() {
    return scmStateMachine.getLastAppliedTermIndex();
  }

  private GroupInfoReply getGroupInfo() throws IOException {
    GroupInfoRequest groupInfoRequest = new GroupInfoRequest(clientId,
        raftPeerId, raftGroupId, nextCallId());
    GroupInfoReply groupInfo = server.getGroupInfo(groupInfoRequest);
    return groupInfo;
  }

  private void setServerRole(RaftPeerRole currentRole,
                             RaftPeerId leaderPeerId) {
    this.roleCheckLock.writeLock().lock();
    try {
      this.cachedPeerRole = Optional.ofNullable(currentRole);
      this.cachedLeaderPeerId = Optional.ofNullable(leaderPeerId);
    } finally {
      this.roleCheckLock.writeLock().unlock();
    }
  }

  /**
   * Configuration used by SCM Ratis Server.
   */
  @ConfigGroup(prefix = "ozone.scm.ratis")
  public static class SCMRatisServerConfiguration {
    @Config(key = "rpc.type",
        type = ConfigType.STRING,
        defaultValue = "GRPC",
        tags = {SCM, OZONE, HA, RATIS},
        description = "Ratis supports different kinds of transports like" +
            " netty, GRPC， Hadoop RPC etc. This picks one of those for" +
            " this cluster."
    )
    private String rpcType;

    @Config(key = "segment.size",
        type = ConfigType.SIZE,
        defaultValue = "16KB",
        tags = {SCM, OZONE, HA, RATIS},
        description = "The size of the raft segment used by Apache Ratis on" +
            " SCM. (16 KB by default)"
    )
    private double segmentSize = 16 * 1024;

    @Config(key = "segment.preallocated.size",
        type = ConfigType.SIZE,
        defaultValue = "16KB",
        tags = {SCM, OZONE, HA, RATIS},
        description = "The size of the buffer which is preallocated for" +
            " raft segment used by Apache Ratis on SCM.(16 KB by default)"
    )
    private double preallocatedSize = 16 * 1024;

    @Config(key = "log.appender.queue.num-elements",
        type = ConfigType.INT,
        defaultValue = "1024",
        tags = {SCM, OZONE, HA, RATIS},
        description = "Number of operation pending with Raft's Log Worker."
    )
    private int logAppenderQueueNum = 1024;

    @Config(key = "log.appender.queue.byte-limit",
        type = ConfigType.SIZE,
        defaultValue = "32MB",
        tags = {SCM, OZONE, HA, RATIS},
        description = "Byte limit for Raft's Log Worker queue."
    )
    private double logAppenderQueueByteLimit = 32 * 1024 * 1024;

    @Config(key = "log.purge.gap",
        type = ConfigType.INT,
        defaultValue = "1000000",
        tags = {SCM, OZONE, HA, RATIS},
        description = "The minimum gap between log indices for Raft server to" +
            " purge its log segments after taking snapshot."
    )
    private int logPurgeGap = 1000000;

    @Config(key = "server.request.timeout",
        type = ConfigType.TIME,
        defaultValue = "3s",
        tags = {SCM, OZONE, HA, RATIS},
        description = "The timeout duration for SCM's ratis server request."
    )
    private long requestTimeout = 3 * 1000L;

    @Config(key = "server.retry.cache.timeout",
        type = ConfigType.TIME,
        defaultValue = "60s",
        tags = {SCM, OZONE, HA, RATIS},
        description = "Retry Cache entry timeout for SCM's ratis server."
    )
    private long retryCacheTimeout = 60 * 1000L;

    @Config(key = "minimum.timeout",
        type = ConfigType.TIME,
        defaultValue = "1s",
        tags = {SCM, OZONE, HA, RATIS},
        description = "The minimum timeout duration for SCM's Ratis server rpc."
    )
    private long minTimeout = 1 * 1000L;

    @Config(key = "leader.election.minimum.timeout.duration",
        type = ConfigType.TIME,
        defaultValue = "1s",
        tags = {SCM, OZONE, HA, RATIS},
        description = "The minimum timeout duration for SCM ratis leader" +
            " election. Default is 1s."
    )
    private long minLeaderElectionTimeout = 1 * 1000L;

    @Config(key = "server.failure.timeout.duration",
        type = ConfigType.TIME,
        defaultValue = "120s",
        tags = {SCM, OZONE, HA, RATIS},
        description = "The timeout duration for ratis server failure" +
            " detection, once the threshold has reached, the ratis state" +
            " machine will be informed about the failure in the ratis ring."
    )
    private long failureTimeout = 120 * 1000L;

    @Config(key = "server.role.check.interval",
        type = ConfigType.TIME,
        defaultValue = "15s",
        tags = {SCM, OZONE, HA, RATIS},
        description = "The interval between SCM leader performing a role" +
            " check on its ratis server. Ratis server informs SCM if it loses" +
            " the leader role. The scheduled check is an secondary check to" +
            " ensure that the leader role is updated periodically"
    )
    private long roleCheckerInterval = 15 * 1000L;

    public String getRpcType() {
      return rpcType;
    }

    public double getSegmentSize() {
      return segmentSize;
    }

    public double getPreallocatedSize() {
      return preallocatedSize;
    }

    public int getLogAppenderQueueNum() {
      return logAppenderQueueNum;
    }

    public double getLogAppenderQueueByteLimit() {
      return logAppenderQueueByteLimit;
    }

    public int getLogPurgeGap() {
      return logPurgeGap;
    }

    public long getRequestTimeout() {
      return requestTimeout;
    }

    public long getRetryCacheTimeout() {
      return retryCacheTimeout;
    }

    public long getMinTimeout() {
      return minTimeout;
    }

    public long getMinLeaderElectionTimeout() {
      return minLeaderElectionTimeout;
    }

    public long getFailureTimeout() {
      return failureTimeout;
    }


    public long getRoleCheckerInterval() {
      return roleCheckerInterval;
    }
  }
}
