/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.ha;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeType;
import org.apache.hadoop.hdds.ratis.ServerNotLeaderException;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;
import org.apache.hadoop.hdds.scm.pipeline.PipelineNotFoundException;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ipc_.RemoteException;
import org.apache.hadoop.ozone.ha.ConfUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.ratis.protocol.exceptions.LeaderNotReadyException;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.apache.ratis.protocol.exceptions.ReconfigurationInProgressException;
import org.apache.ratis.protocol.exceptions.ReconfigurationTimeoutException;
import org.apache.ratis.protocol.exceptions.ResourceUnavailableException;
import org.apache.ratis.protocol.exceptions.StateMachineException;

/**
 * Utility class used by SCM HA.
 */
public final class SCMHAUtils {

  private static final ImmutableList<Class<? extends Exception>>
      RETRIABLE_WITH_NO_FAILOVER_EXCEPTION_LIST =
      ImmutableList.<Class<? extends Exception>>builder()
          .add(LeaderNotReadyException.class)
          .add(ReconfigurationInProgressException.class)
          .add(ReconfigurationTimeoutException.class)
          .add(ResourceUnavailableException.class)
          .build();

  private static final ImmutableList<Class<? extends Exception>>
      NON_RETRIABLE_EXCEPTION_LIST =
      ImmutableList.<Class<? extends Exception>>builder()
          .add(SCMException.class)
          .add(NonRetriableException.class)
          .add(PipelineNotFoundException.class)
          .add(ContainerNotFoundException.class)
          .build();

  private SCMHAUtils() {
    // not used
  }

  public static String getPrimordialSCM(ConfigurationSource conf) {
    return conf.get(ScmConfigKeys.OZONE_SCM_PRIMORDIAL_NODE_ID_KEY);
  }

  public static boolean isPrimordialSCM(ConfigurationSource conf,
      String selfNodeId, String hostName) {
    String primordialNode = getPrimordialSCM(conf);
    return primordialNode != null && (primordialNode
        .equals(selfNodeId) || primordialNode.equals(hostName));
  }

  /**
   * Get the local directory where ratis logs will be stored.
   */
  public static String getSCMRatisDirectory(ConfigurationSource conf) {
    String scmRatisDirectory =
            conf.get(ScmConfigKeys.OZONE_SCM_HA_RATIS_STORAGE_DIR);

    if (Strings.isNullOrEmpty(scmRatisDirectory)) {
      scmRatisDirectory = ServerUtils.getDefaultRatisDirectory(conf, NodeType.SCM);
    }
    return scmRatisDirectory;
  }

  public static String getSCMRatisSnapshotDirectory(ConfigurationSource conf) {
    String snapshotDir =
            conf.get(ScmConfigKeys.OZONE_SCM_HA_RATIS_SNAPSHOT_DIR);

    // If ratis snapshot directory is not set, fall back to ozone.metadata.dir with component-specific location.
    if (Strings.isNullOrEmpty(snapshotDir)) {
      snapshotDir = ServerUtils.getDefaultRatisSnapshotDirectory(conf, NodeType.SCM);
    }
    return snapshotDir;
  }

  /**
   * Removes the self node from the list of nodes in the
   * configuration.
   * @param configuration OzoneConfiguration
   * @param selfId - Local node Id of SCM.
   * @return Updated OzoneConfiguration
   */

  public static OzoneConfiguration removeSelfId(
      OzoneConfiguration configuration, String selfId) {
    final OzoneConfiguration conf = new OzoneConfiguration(configuration);
    String scmNodesKey = ConfUtils.addKeySuffixes(
        ScmConfigKeys.OZONE_SCM_NODES_KEY, HddsUtils.getScmServiceId(conf));
    String scmNodes = conf.get(scmNodesKey);
    if (scmNodes != null) {
      String[] parts = scmNodes.split(",");
      List<String> partsLeft = new ArrayList<>();
      for (String part : parts) {
        if (!part.equals(selfId)) {
          partsLeft.add(part);
        }
      }
      conf.set(scmNodesKey, String.join(",", partsLeft));
    }
    return conf;
  }

  public static Throwable unwrapException(Exception e) {
    Throwable cause = e.getCause();
    if (e instanceof RemoteException) {
      return ((RemoteException) e).unwrapRemoteException();
    } else if (cause instanceof RemoteException) {
      return ((RemoteException) cause).unwrapRemoteException();
    }
    return e;
  }

  /**
   * Checks if the underlying exception if of type StateMachine. Used by scm
   * clients.
   */
  public static boolean isNonRetriableException(Exception e) {
    Throwable t =
        getExceptionForClass(e, StateMachineException.class);
    return t != null;
  }

  /**
   * Checks if the underlying exception if of type non retriable. Used by scm
   * clients.
   */
  public static boolean checkNonRetriableException(Exception e) {
    Throwable t = unwrapException(e);
    for (Class<? extends Exception> clazz : NON_RETRIABLE_EXCEPTION_LIST) {
      if (clazz.isInstance(t)) {
        return true;
      }
    }
    return false;
  }

  // This will return the underlying exception after unwrapping
  // the exception to see if it matches with expected exception
  // list , returns true otherwise will return false.
  public static boolean isRetriableWithNoFailoverException(Exception e) {
    Throwable t = e;
    while (t != null) {
      for (Class<? extends Exception> clazz :
          getRetriableWithNoFailoverExceptionList()) {
        if (clazz.isInstance(t)) {
          return true;
        }
      }
      t = t.getCause();
    }
    return false;
  }

  /**
   * Checks if the underlying exception if of type retriable with no failover.
   * Used by scm clients.
   */
  public static boolean checkRetriableWithNoFailoverException(Exception e) {
    Throwable t = unwrapException(e);
    return RetriableWithNoFailoverException.class.isInstance(t);
  }

  public static Throwable getNotLeaderException(Exception e) {
    return getExceptionForClass(e, NotLeaderException.class);
  }

  public static Throwable getServerNotLeaderException(Exception e) {
    return getExceptionForClass(e, ServerNotLeaderException.class);
  }

  // This will return the underlying NotLeaderException exception
  public static Throwable getExceptionForClass(Exception e,
      Class<? extends Exception> clazz) {
    IOException ioException = null;
    Throwable cause = e.getCause();
    if (cause instanceof RemoteException) {
      ioException = ((RemoteException) cause).unwrapRemoteException();
    }
    Throwable t = ioException == null ? e : ioException;
    while (t != null) {
      if (clazz.isInstance(t)) {
        return t;
      }
      t = t.getCause();
    }
    return null;
  }

  private static List<Class<? extends
      Exception>> getRetriableWithNoFailoverExceptionList() {
    return RETRIABLE_WITH_NO_FAILOVER_EXCEPTION_LIST;
  }

  public static RetryPolicy.RetryAction getRetryAction(int failovers, int retry,
      Exception e, int maxRetryCount, long retryInterval) {
    Throwable unwrappedException = HddsUtils.getUnwrappedException(e);
    if (unwrappedException instanceof AccessControlException) {
      // For AccessControl Exception where Client is not authenticated.
      return RetryPolicy.RetryAction.FAIL;
    } else if (HddsUtils.shouldNotFailoverOnRpcException(unwrappedException)) {
      // For some types of Rpc Exceptions, retrying on different server would
      // not help.
      return RetryPolicy.RetryAction.FAIL;
    } else if (SCMHAUtils.checkRetriableWithNoFailoverException(e)) {
      if (retry < maxRetryCount) {
        return new RetryPolicy.RetryAction(
            RetryPolicy.RetryAction.RetryDecision.RETRY, retryInterval);
      } else {
        return RetryPolicy.RetryAction.FAIL;
      }
    } else if (SCMHAUtils.checkNonRetriableException(e)) {
      return RetryPolicy.RetryAction.FAIL;
    } else {
      // For any other exception like RetriableWithFailOverException or any
      // other we perform fail-over and retry.
      if (failovers < maxRetryCount) {
        return new RetryPolicy.RetryAction(
            RetryPolicy.RetryAction.RetryDecision.FAILOVER_AND_RETRY,
            retryInterval);
      } else {
        return RetryPolicy.RetryAction.FAIL;
      }
    }
  }
}
