/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * <p>Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.ha;

import java.net.InetSocketAddress;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigType;
import org.apache.hadoop.net.NetUtils;

import static org.apache.hadoop.hdds.conf.ConfigTag.HA;
import static org.apache.hadoop.hdds.conf.ConfigTag.OZONE;
import static org.apache.hadoop.hdds.conf.ConfigTag.RATIS;
import static org.apache.hadoop.hdds.conf.ConfigTag.SCM;

/**
 * Configuration used by SCM HA.
 */
@ConfigGroup(prefix = "ozone.scm.ha")
public class SCMHAConfiguration {

  @Config(key = "ratis.storage.dir",
      type = ConfigType.STRING,
      defaultValue = "",
      tags = {OZONE, SCM, HA, RATIS},
      description = "Storage directory used by SCM to write Ratis logs."
  )
  private String ratisStorageDir;

  @Config(key = "ratis.bind.host",
      type = ConfigType.STRING,
      defaultValue = "0.0.0.0",
      tags = {OZONE, SCM, HA, RATIS},
      description = "Host used by SCM for binding Ratis Server."
  )
  private String ratisBindHost = "0.0.0.0";

  @Config(key = "ratis.bind.port",
      type = ConfigType.INT,
      defaultValue = "9894",
      tags = {OZONE, SCM, HA, RATIS},
      description = "Port used by SCM for Ratis Server."
  )
  private int ratisBindPort = 9894;


  @Config(key = "ratis.rpc.type",
      type = ConfigType.STRING,
      defaultValue = "GRPC",
      tags = {SCM, OZONE, HA, RATIS},
      description = "Ratis supports different kinds of transports like" +
          " netty, GRPC， Hadoop RPC etc. This picks one of those for" +
          " this cluster."
  )
  private String ratisRpcType;

  @Config(key = "ratis.segment.size",
      type = ConfigType.SIZE,
      defaultValue = "16KB",
      tags = {SCM, OZONE, HA, RATIS},
      description = "The size of the raft segment used by Apache Ratis on" +
          " SCM. (16 KB by default)"
  )
  private long raftSegmentSize = 16L * 1024L;

  @Config(key = "ratis.segment.preallocated.size",
      type = ConfigType.SIZE,
      defaultValue = "16KB",
      tags = {SCM, OZONE, HA, RATIS},
      description = "The size of the buffer which is preallocated for" +
          " raft segment used by Apache Ratis on SCM.(16 KB by default)"
  )
  private long raftSegmentPreAllocatedSize = 16L * 1024L;

  @Config(key = "ratis.log.appender.queue.num-elements",
      type = ConfigType.INT,
      defaultValue = "1024",
      tags = {SCM, OZONE, HA, RATIS},
      description = "Number of operation pending with Raft's Log Worker."
  )
  private int raftLogAppenderQueueNum = 1024;

  @Config(key = "ratis.log.appender.queue.byte-limit",
      type = ConfigType.SIZE,
      defaultValue = "32MB",
      tags = {SCM, OZONE, HA, RATIS},
      description = "Byte limit for Raft's Log Worker queue."
  )
  private int raftLogAppenderQueueByteLimit = 32 * 1024 * 1024;

  @Config(key = "ratis.log.purge.enabled",
      type = ConfigType.BOOLEAN,
      defaultValue = "false",
      tags = {SCM, OZONE, HA, RATIS},
      description = "whether enable raft log purge."
  )
  private boolean raftLogPurgeEnabled = false;

  @Config(key = "ratis.log.purge.gap",
      type = ConfigType.INT,
      defaultValue = "1000000",
      tags = {SCM, OZONE, HA, RATIS},
      description = "The minimum gap between log indices for Raft server to" +
          " purge its log segments after taking snapshot."
  )
  private int raftLogPurgeGap = 1000000;

  @Config(key = "ratis.snapshot.threshold",
      type = ConfigType.LONG,
      defaultValue = "1000",
      tags = {SCM, OZONE, HA, RATIS},
      description = "The threshold to trigger a Ratis taking snapshot " +
          "operation for SCM")
  private long ratisSnapshotThreshold = 1000L;

  @Config(key = "ratis.request.timeout",
      type = ConfigType.TIME,
      defaultValue = "3000ms",
      tags = {SCM, OZONE, HA, RATIS},
      description = "The timeout duration for SCM's Ratis server RPC."
  )
  private long ratisRequestTimeout = 3000L;

  @Config(key = "ratis.server.retry.cache.timeout",
      type = ConfigType.TIME,
      defaultValue = "60s",
      tags = {SCM, OZONE, HA, RATIS},
      description = "Retry Cache entry timeout for SCM's ratis server."
  )
  private long ratisRetryCacheTimeout = 60 * 1000L;


  @Config(key = "ratis.leader.election.timeout",
      type = ConfigType.TIME,
      defaultValue = "5s",
      tags = {SCM, OZONE, HA, RATIS},
      description = "The minimum timeout duration for SCM ratis leader" +
          " election. Default is 1s."
  )
  private long ratisLeaderElectionTimeout = 5 * 1000L;

  @Config(key = "ratis.leader.ready.wait.timeout",
      type = ConfigType.TIME,
      defaultValue = "60s",
      tags = {SCM, OZONE, HA, RATIS},
      description = "The minimum timeout duration for waiting for" +
                    "leader readiness"
  )
  private long ratisLeaderReadyWaitTimeout = 60 * 1000L;

  @Config(key = "ratis.leader.ready.check.interval",
      type = ConfigType.TIME,
      defaultValue = "2s",
      tags = {SCM, OZONE, HA, RATIS},
      description = "The interval between ratis server performing" +
                    "a leader readiness check"
  )
  private long ratisLeaderReadyCheckInterval = 2 * 1000L;

  @Config(key = "ratis.server.failure.timeout.duration",
      type = ConfigType.TIME,
      defaultValue = "120s",
      tags = {SCM, OZONE, HA, RATIS},
      description = "The timeout duration for ratis server failure" +
          " detection, once the threshold has reached, the ratis state" +
          " machine will be informed about the failure in the ratis ring."
  )
  private long ratisNodeFailureTimeout = 120 * 1000L;

  @Config(key = "ratis.server.role.check.interval",
      type = ConfigType.TIME,
      defaultValue = "15s",
      tags = {SCM, OZONE, HA, RATIS},
      description = "The interval between SCM leader performing a role" +
          " check on its ratis server. Ratis server informs SCM if it loses" +
          " the leader role. The scheduled check is an secondary check to" +
          " ensure that the leader role is updated periodically"
  )
  private long ratisRoleCheckerInterval = 15 * 1000L;

  @Config(key = "ratis.snapshot.dir",
      type = ConfigType.STRING,
      defaultValue = "",
      tags = {SCM, OZONE, HA, RATIS},
      description = "The ratis snapshot dir location"
  )
  private String ratisSnapshotDir;

  @Config(key = "grpc.deadline.interval",
      type = ConfigType.TIME,
      defaultValue = "30m",
      tags = {OZONE, SCM, HA, RATIS},
      description = "Deadline for SCM DB checkpoint interval."
  )
  private long grpcDeadlineInterval = 30 * 60 * 1000L;

  public long getGrpcDeadlineInterval() {
    return grpcDeadlineInterval;
  }


  public String getRatisStorageDir() {
    return ratisStorageDir;
  }

  public String getRatisSnapshotDir() {
    return ratisSnapshotDir;
  }

  public void setRatisStorageDir(String dir) {
    this.ratisStorageDir = dir;
  }

  public void setRatisSnapshotDir(String dir) {
    this.ratisSnapshotDir = dir;
  }

  public void setRaftLogPurgeGap(int gap) {
    this.raftLogPurgeGap = gap;
  }

  public InetSocketAddress getRatisBindAddress() {
    return NetUtils.createSocketAddr(ratisBindHost, ratisBindPort);
  }

  public String getRatisRpcType() {
    return ratisRpcType;
  }

  public long getRaftSegmentSize() {
    return raftSegmentSize;
  }

  public long getRaftSegmentPreAllocatedSize() {
    return raftSegmentPreAllocatedSize;
  }

  public int getRaftLogAppenderQueueNum() {
    return raftLogAppenderQueueNum;
  }

  public int getRaftLogAppenderQueueByteLimit() {
    return raftLogAppenderQueueByteLimit;
  }

  public boolean getRaftLogPurgeEnabled() {
    return raftLogPurgeEnabled;
  }

  public void setRaftLogPurgeEnabled(boolean enabled) {
    this.raftLogPurgeEnabled = enabled;
  }

  public int getRaftLogPurgeGap() {
    return raftLogPurgeGap;
  }

  public long getRatisSnapshotThreshold() {
    return ratisSnapshotThreshold;
  }

  public void setRatisSnapshotThreshold(long threshold) {
    this.ratisSnapshotThreshold = threshold;
  }

  public long getRatisRetryCacheTimeout() {
    return ratisRetryCacheTimeout;
  }

  public long getRatisRequestTimeout() {
    Preconditions.checkArgument(ratisRequestTimeout > 1000L,
        "Ratis request timeout cannot be less than 1000ms.");
    return ratisRequestTimeout;
  }

  public long getLeaderElectionMinTimeout() {
    return ratisLeaderElectionTimeout;
  }

  public long getLeaderElectionMaxTimeout() {
    return ratisLeaderElectionTimeout + 200L;
  }

  public long getLeaderReadyWaitTimeout() {
    return ratisLeaderReadyWaitTimeout;
  }

  public void setLeaderReadyWaitTimeout(long mills) {
    ratisLeaderReadyWaitTimeout = mills;
  }

  public long getLeaderReadyCheckInterval() {
    return ratisLeaderReadyCheckInterval;
  }

  public long getRatisNodeFailureTimeout() {
    return ratisNodeFailureTimeout;
  }

  public long getRatisRoleCheckerInterval() {
    return ratisRoleCheckerInterval;
  }
}