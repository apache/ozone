/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.ha;

import com.google.common.base.Strings;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.rpc.RpcType;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;

import java.io.File;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.apache.ratis.server.RaftServerConfigKeys.Log;
import static org.apache.ratis.server.RaftServerConfigKeys.RetryCache;
import static org.apache.ratis.server.RaftServerConfigKeys.Rpc;
import static org.apache.ratis.server.RaftServerConfigKeys.Snapshot;

/**
 * Ratis Util for SCM HA.
 */
public final class RatisUtil {

  private RatisUtil() {
  }


  /**
   * Constructs new Raft Properties instance using {@link SCMHAConfiguration}.
   * @param haConf SCMHAConfiguration
   * @param conf ConfigurationSource
   */
  public static RaftProperties newRaftProperties(
      final SCMHAConfiguration haConf, final ConfigurationSource conf) {
    //TODO: Remove ConfigurationSource!
    // TODO: Check the default values.
    final RaftProperties properties = new RaftProperties();
    setRaftStorageDir(properties, haConf, conf);
    setRaftRpcProperties(properties, haConf);
    setRaftLogProperties(properties, haConf);
    setRaftRetryCacheProperties(properties, haConf);
    setRaftSnapshotProperties(properties, haConf);
    return properties;
  }

  /**
   * Set the local directory where ratis logs will be stored.
   *
   * @param properties RaftProperties instance which will be updated
   * @param haConf SCMHAConfiguration
   * @param conf ConfigurationSource
   */
  public static void setRaftStorageDir(final RaftProperties properties,
                                       final SCMHAConfiguration haConf,
                                       final ConfigurationSource conf) {
    String storageDir = haConf.getRatisStorageDir();
    if (Strings.isNullOrEmpty(storageDir)) {
      File metaDirPath = ServerUtils.getOzoneMetaDirPath(conf);
      storageDir = (new File(metaDirPath, "scm-ha")).getPath();
    }
    RaftServerConfigKeys.setStorageDir(properties,
        Collections.singletonList(new File(storageDir)));
  }

  /**
   * Set properties related to Raft RPC.
   *
   * @param properties RaftProperties instance which will be updated
   * @param conf SCMHAConfiguration
   */
  private static void setRaftRpcProperties(final RaftProperties properties,
                                           final SCMHAConfiguration conf) {
    RaftConfigKeys.Rpc.setType(properties,
        RpcType.valueOf(conf.getRatisRpcType()));
    GrpcConfigKeys.Server.setPort(properties,
        conf.getRatisBindAddress().getPort());
    GrpcConfigKeys.setMessageSizeMax(properties,
        SizeInBytes.valueOf("32m"));

    Rpc.setRequestTimeout(properties, TimeDuration.valueOf(
        conf.getRatisRequestTimeout(), TimeUnit.MILLISECONDS));
    Rpc.setTimeoutMin(properties, TimeDuration.valueOf(
        conf.getLeaderElectionMinTimeout(), TimeUnit.MILLISECONDS));
    Rpc.setTimeoutMax(properties, TimeDuration.valueOf(
        conf.getLeaderElectionMaxTimeout(), TimeUnit.MILLISECONDS));
    Rpc.setSlownessTimeout(properties, TimeDuration.valueOf(
        conf.getRatisNodeFailureTimeout(), TimeUnit.MILLISECONDS));
  }

  /**
   * Set properties related to Raft Log.
   *
   * @param properties RaftProperties instance which will be updated
   * @param conf SCMHAConfiguration
   */
  private static void setRaftLogProperties(final RaftProperties properties,
                                           final SCMHAConfiguration conf) {
    Log.setSegmentSizeMax(properties,
        SizeInBytes.valueOf(conf.getRaftSegmentSize()));
    Log.Appender.setBufferElementLimit(properties,
        conf.getRaftLogAppenderQueueByteLimit());
    Log.Appender.setBufferByteLimit(properties,
        SizeInBytes.valueOf(conf.getRaftLogAppenderQueueByteLimit()));
    Log.setPreallocatedSize(properties,
        SizeInBytes.valueOf(conf.getRaftSegmentPreAllocatedSize()));
    Log.Appender.setInstallSnapshotEnabled(properties, false);
    Log.setPurgeUptoSnapshotIndex(properties, conf.getRaftLogPurgeEnabled());
    Log.setPurgeGap(properties, conf.getRaftLogPurgeGap());
    Log.setSegmentCacheNumMax(properties, 2);
  }

  /**
   * Set properties related to Raft Retry Cache.
   *
   * @param properties RaftProperties instance which will be updated
   * @param conf SCMHAConfiguration
   */
  private static void setRaftRetryCacheProperties(
      final RaftProperties properties, final SCMHAConfiguration conf) {
    RetryCache.setExpiryTime(properties, TimeDuration.valueOf(
        conf.getRatisRetryCacheTimeout(), TimeUnit.MILLISECONDS));
  }

  /**
   * Set properties related to Raft Snapshot.
   *
   * @param properties RaftProperties instance which will be updated
   * @param conf SCMHAConfiguration
   */
  private static void setRaftSnapshotProperties(
      final RaftProperties properties, final SCMHAConfiguration conf) {
    Snapshot.setAutoTriggerEnabled(properties, true);
    Snapshot.setAutoTriggerThreshold(properties,
        conf.getRatisSnapshotThreshold());
  }

}
