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
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class RatisUtil {

  private RatisUtil() {
  }


  //TODO: Remove ConfigurationSource!
  public static RaftProperties newRaftProperties(SCMHAConfiguration haConf, ConfigurationSource conf) {
    // TODO: Check the default values.
    // TODO: Use configuration to read all the values.
    final RaftProperties properties = new RaftProperties();

    final InetSocketAddress address = haConf.getRatisBindAddress();
    RaftConfigKeys.Rpc.setType(properties, RpcType.valueOf(haConf.getRatisRpcType()));
    GrpcConfigKeys.Server.setPort(properties, address.getPort());
    RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(getRatisStorageDirectory(haConf, conf)));
    RaftServerConfigKeys.Log.setSegmentSizeMax(properties, SizeInBytes.valueOf(haConf.getRaftSegmentSize()));
    RaftServerConfigKeys.Log.Appender.setBufferElementLimit(properties, haConf.getLogAppenderQueueByteLimit());
    RaftServerConfigKeys.Log.Appender.setBufferByteLimit(properties, SizeInBytes.valueOf(haConf.getLogAppenderQueueByteLimit()));
    RaftServerConfigKeys.Log.setPreallocatedSize(properties, SizeInBytes.valueOf(haConf.getPreallocatedSize()));
    RaftServerConfigKeys.Log.Appender.setInstallSnapshotEnabled(properties, false);
    RaftServerConfigKeys.Log.setPurgeGap(properties, 1000000);
    GrpcConfigKeys.setMessageSizeMax(properties, SizeInBytes.valueOf("32m"));
    TimeDuration serverRequestTimeout = TimeDuration.valueOf(3000, TimeUnit.MILLISECONDS);
    RaftServerConfigKeys.Rpc.setRequestTimeout(properties, serverRequestTimeout);
    TimeDuration retryCacheTimeout = TimeDuration.valueOf(600000, TimeUnit.MILLISECONDS);
    RaftServerConfigKeys.RetryCache.setExpiryTime(properties, retryCacheTimeout);
    TimeDuration serverMinTimeout = TimeDuration.valueOf(1, TimeUnit.SECONDS);
    long serverMaxTimeoutDuration = serverMinTimeout.toLong(TimeUnit.MILLISECONDS) + 200;
    final TimeDuration serverMaxTimeout = TimeDuration.valueOf(serverMaxTimeoutDuration, TimeUnit.SECONDS);
    RaftServerConfigKeys.Rpc.setTimeoutMin(properties, serverMinTimeout);
    RaftServerConfigKeys.Rpc.setTimeoutMax(properties, serverMaxTimeout);
    RaftServerConfigKeys.Log.setSegmentCacheNumMax(properties, 2);
    TimeDuration leaderElectionMinTimeout = TimeDuration.valueOf(1, TimeUnit.SECONDS);
    RaftServerConfigKeys.Rpc.setTimeoutMin(properties, leaderElectionMinTimeout);
    long leaderElectionMaxTimeout = leaderElectionMinTimeout.toLong(TimeUnit.MILLISECONDS) + 200;
    RaftServerConfigKeys.Rpc.setTimeoutMax(properties, TimeDuration.valueOf(leaderElectionMaxTimeout, TimeUnit.MILLISECONDS));
    TimeDuration nodeFailureTimeout = TimeDuration.valueOf(120, TimeUnit.SECONDS);
    RaftServerConfigKeys.Notification.setNoLeaderTimeout(properties, nodeFailureTimeout);
    RaftServerConfigKeys.Rpc.setSlownessTimeout(properties, nodeFailureTimeout);
    RaftServerConfigKeys.Snapshot.setAutoTriggerEnabled(properties, true);
    RaftServerConfigKeys.Snapshot.setAutoTriggerThreshold(properties, 400000);

    return properties;
  }

  /**
   * Get the local directory where ratis logs will be stored.
   */
  public static File getRatisStorageDirectory(SCMHAConfiguration haConf, ConfigurationSource conf) {
    String storageDir = haConf.getRatisStorageDir();
    if (Strings.isNullOrEmpty(storageDir)) {
      storageDir = ServerUtils.getDefaultRatisDirectory(conf);
    }
    return new File(storageDir);
  }
}
