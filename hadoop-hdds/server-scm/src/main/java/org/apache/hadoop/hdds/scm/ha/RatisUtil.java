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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.ratis.ServerNotLeaderException;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.security.exception.SCMSecurityException;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.apache.ratis.rpc.RpcType;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdds.security.exception.SCMSecurityException.ErrorCode.GET_DN_CERTIFICATE_FAILED;
import static org.apache.hadoop.hdds.security.exception.SCMSecurityException.ErrorCode.GET_OM_CERTIFICATE_FAILED;
import static org.apache.hadoop.hdds.security.exception.SCMSecurityException.ErrorCode.GET_SCM_CERTIFICATE_FAILED;
import static org.apache.hadoop.hdds.security.exception.SCMSecurityException.ErrorCode.NOT_A_PRIMARY_SCM;
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
    setRaftRpcProperties(properties, haConf, conf);
    setRaftLogProperties(properties, haConf, conf);
    setRaftRetryCacheProperties(properties, haConf, conf);
    setRaftSnapshotProperties(properties, haConf, conf);
    setRaftLeadElectionProperties(properties, haConf);
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
    RaftServerConfigKeys.setStorageDir(properties,
        Collections.singletonList(new File(getRatisStorageDir(haConf, conf))));
  }

  public static String getRatisStorageDir(final SCMHAConfiguration haConf,
      final ConfigurationSource conf) {
    String storageDir = conf.get(ScmConfigKeys.RATIS_STORAGE_DIR);
    if (Strings.isNullOrEmpty(storageDir)) {
      File metaDirPath = ServerUtils.getOzoneMetaDirPath(conf);
      storageDir = (new File(metaDirPath, "scm-ha")).getPath();
    }
    return storageDir;
  }
  /**
   * Set properties related to Raft RPC.
   *
   * @param properties RaftProperties instance which will be updated
   * @param conf SCMHAConfiguration
   */
  private static void setRaftRpcProperties(final RaftProperties properties,
      final SCMHAConfiguration conf, ConfigurationSource ozoneConf) {
    RaftConfigKeys.Rpc.setType(properties,
        RpcType.valueOf(ozoneConf.get(ScmConfigKeys.RATIS_RPC_TYPE,
                ScmConfigKeys.RATIS_RPC_TYPE_DEFAULT)));
    GrpcConfigKeys.Server.setPort(properties, ozoneConf
        .getInt(ScmConfigKeys.OZONE_SCM_RATIS_PORT_KEY,
            ScmConfigKeys.OZONE_SCM_RATIS_PORT_DEFAULT));
    GrpcConfigKeys.setMessageSizeMax(properties,
        SizeInBytes.valueOf("32m"));
    long ratisRequestTimeout = ozoneConf.getLong(
            ScmConfigKeys.RATIS_REQUEST_TIMEOUT,
            ScmConfigKeys.RATIS_REQUEST_TIMEOUT_DEFAULT);
    Preconditions.checkArgument(ratisRequestTimeout > 1000L,
            "Ratis request timeout cannot be less than 1000ms.");
    Rpc.setRequestTimeout(properties, TimeDuration.valueOf(
        ratisRequestTimeout, TimeUnit.MILLISECONDS));
    Rpc.setTimeoutMin(properties, TimeDuration.valueOf(
        ozoneConf.getLong(ScmConfigKeys.RATIS_LEADER_ELECTION_TIMEOUT,
                ScmConfigKeys.RATIS_LEADER_ELECTION_TIMEOUT_DEFAULT),
            TimeUnit.MILLISECONDS));
    Rpc.setTimeoutMax(properties, TimeDuration.valueOf(
        ozoneConf.getLong(ScmConfigKeys.RATIS_LEADER_ELECTION_TIMEOUT,
                ScmConfigKeys.RATIS_LEADER_ELECTION_TIMEOUT_DEFAULT)+200L,
            TimeUnit.MILLISECONDS));
    Rpc.setSlownessTimeout(properties, TimeDuration.valueOf(
            ozoneConf.getLong(ScmConfigKeys.RATIS_NODE_FAILURE_TIMEOUT,
                    ScmConfigKeys.RATIS_NODE_FAILURE_TIMEOUT_DEFAULT),
            TimeUnit.MILLISECONDS));
  }

  /**
   * Set properties related to Raft leader election.
   *
   * @param properties RaftProperties instance which will be updated
   * @param conf SCMHAConfiguration
   */
  private static void setRaftLeadElectionProperties(
      final RaftProperties properties, final SCMHAConfiguration conf) {
    // Disable the pre vote feature (related to leader election) in Ratis
    RaftServerConfigKeys.LeaderElection.setPreVote(properties, false);
  }

  /**
   * Set properties related to Raft Log.
   *
   * @param properties RaftProperties instance which will be updated
   * @param conf SCMHAConfiguration
   */
  private static void setRaftLogProperties(final RaftProperties properties,
      final SCMHAConfiguration conf, final ConfigurationSource ozoneConf) {
    Log.setSegmentSizeMax(properties,
        SizeInBytes.valueOf(ozoneConf.getLong(ScmConfigKeys.RAFT_SEGMENT_SIZE,
                ScmConfigKeys.RAFT_SEGMENT_SIZE_DEFAULT)));
    Log.Appender.setBufferElementLimit(properties,
            ozoneConf.getInt(ScmConfigKeys.RAFT_LOG_APPENDER_QUEUE_BYTE_LIMIT,
                    ScmConfigKeys.RAFT_LOG_APPENDER_QUEUE_BYTE_LIMIT_DEFAULT));
    Log.Appender.setBufferByteLimit(properties,
        SizeInBytes.valueOf(
            ozoneConf.getInt(ScmConfigKeys.RAFT_LOG_APPENDER_QUEUE_BYTE_LIMIT,
                    ScmConfigKeys.RAFT_LOG_APPENDER_QUEUE_BYTE_LIMIT_DEFAULT)));
    Log.setPreallocatedSize(properties,
        SizeInBytes.valueOf(
            ozoneConf.getLong(ScmConfigKeys.RAFT_SEGMENT_PRE_ALLOCATED_SIZE,
                    ScmConfigKeys.RAFT_SEGMENT_PRE_ALLOCATED_SIZE_DEFAULT)));
    Log.Appender.setInstallSnapshotEnabled(properties, false);
    Log.setPurgeUptoSnapshotIndex(properties,
            ozoneConf.getBoolean(ScmConfigKeys.RAFT_LOG_PURGE_ENABLED,
                    ScmConfigKeys.RAFT_LOG_PURGE_ENABLED_DEFAULT));
    Log.setPurgeGap(properties,
            ozoneConf.getInt(ScmConfigKeys.RAFT_LOG_PURGE_GAP,
                    ScmConfigKeys.RAFT_LOG_PURGE_GAP_DEFAULT));
    Log.setSegmentCacheNumMax(properties, 2);
  }

  /**
   * Set properties related to Raft Retry Cache.
   *
   * @param properties RaftProperties instance which will be updated
   * @param conf SCMHAConfiguration
   */
  private static void setRaftRetryCacheProperties(
      final RaftProperties properties, final SCMHAConfiguration conf,
      final ConfigurationSource ozoneConf) {
    RetryCache.setExpiryTime(properties, TimeDuration.valueOf(
            ozoneConf.getLong(ScmConfigKeys.RATIS_RETRY_CACHE_TIMEOUT,
                    ScmConfigKeys.RATIS_RETRY_CACHE_TIMEOUT_DEFAULT),
            TimeUnit.MILLISECONDS));
  }

  /**
   * Set properties related to Raft Snapshot.
   *
   * @param properties RaftProperties instance which will be updated
   * @param conf SCMHAConfiguration
   */
  private static void setRaftSnapshotProperties(
      final RaftProperties properties, final SCMHAConfiguration conf,
      final ConfigurationSource ozoneConf) {
    Snapshot.setAutoTriggerEnabled(properties, true);
    Snapshot.setAutoTriggerThreshold(properties,
        ozoneConf.getLong(ScmConfigKeys.RATIS_SNAPSHOT_THRESHOLD,
                ScmConfigKeys.RATIS_SNAPSHOT_THRESHOLD_DEFAULT));
  }

  public static void checkRatisException(IOException e, String port,
      String scmId) throws ServiceException {
    if (SCMHAUtils.isNonRetriableException(e)) {
      throw new ServiceException(new NonRetriableException(e));
    } else if (SCMHAUtils.isRetriableWithNoFailoverException(e)) {
      throw new ServiceException(new RetriableWithNoFailoverException(e));
    } else if (SCMHAUtils.getNotLeaderException(e) != null) {
      NotLeaderException nle =
          (NotLeaderException) SCMHAUtils.getNotLeaderException(e);
      throw new ServiceException(ServerNotLeaderException
          .convertToNotLeaderException(nle,
              SCMRatisServerImpl.getSelfPeerId(scmId), port));
    } else if (e instanceof SCMSecurityException) {
      // For NOT_A_PRIMARY_SCM error client needs to retry on next SCM.
      // GetSCMCertificate call can happen on non-leader SCM and only an
      // primary SCM. When the bootstrapped SCM connects to other
      // bootstrapped SCM we get the NOT_A_PRIMARY_SCM. In this scenario
      // client needs to retry next SCM.

      // And also on primary/leader SCM if it failed due to any other reason
      // retry again.
      SCMSecurityException ex = (SCMSecurityException) e;
      if (ex.getErrorCode().equals(NOT_A_PRIMARY_SCM)) {
        throw new ServiceException(new RetriableWithFailOverException(e));
      } else if (ex.getErrorCode().equals(GET_SCM_CERTIFICATE_FAILED) ||
          ex.getErrorCode().equals(GET_OM_CERTIFICATE_FAILED) ||
          ex.getErrorCode().equals(GET_DN_CERTIFICATE_FAILED)) {
        throw new ServiceException(new RetriableWithNoFailoverException(e));
      }
    }
  }
}
