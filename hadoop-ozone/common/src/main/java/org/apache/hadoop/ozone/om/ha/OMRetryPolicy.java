/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.om.ha;

import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ozone.om.exceptions.OMLeaderNotReadyException;
import org.apache.hadoop.ozone.om.exceptions.OMNotLeaderException;
import org.apache.ratis.protocol.exceptions.ReconfigurationInProgressException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Retry Policies for OM proxies.
 */
public class OMRetryPolicy {

  public static final Logger LOG = LoggerFactory.getLogger(
      OMRetryPolicy.class);

  private final int maxFailovers;

  private final HashMap<ExceptionType, RetryAction> defaultRetryRulesMap;
  private HashMap<ExceptionType, RetryAction> retryRulesMapByType;
  private HashMap<Class, RetryAction> retryRulesMapByClass;

  public enum ExceptionType {
    NOT_LEADER,
    LEADER_NOT_READY,
    RECONFIGURATION_IN_PROGRESS
  }

  public enum RetryAction {
    FAILOVER_AND_RETRY,
    RETRY_ON_SAME_OM,
    FAIL,
    EXHAUSTED_MAX_FAILOVER_ATTEMPTS,
    UNDETERMINED
  }

  public OMRetryPolicy(int maxNumFailovers) {
    this.maxFailovers = maxNumFailovers;

    this.defaultRetryRulesMap = new HashMap<>();
    this.retryRulesMapByType = new HashMap<>();
    this.retryRulesMapByClass = new HashMap<>();
    this.defaultRetryRulesMap.put(ExceptionType.NOT_LEADER,
        RetryAction.FAILOVER_AND_RETRY);
    this.defaultRetryRulesMap.put(ExceptionType.LEADER_NOT_READY,
        RetryAction.RETRY_ON_SAME_OM);
  }

  /**
   * Add a Retry rule based on ExceptionType.
   */
  public void addRetryRule(ExceptionType exceptionType,
      RetryAction retryAction) {
    retryRulesMapByType.put(exceptionType, retryAction);
  }

  /**
   * Add a Retry rule based on Exception Class.
   */
  public void addRetryRule(Class exceptionClass,
      RetryAction  retryAction) {
    if (retryRulesMapByClass == null) {
      retryRulesMapByClass = new HashMap<>();
    }
    retryRulesMapByClass.put(exceptionClass, retryAction);
  }

  /**
   * Get the Retry Action based on Exception and failover count.
   */
  public RetryAction getRetryAction(Exception exception, int failovers,
      String currentOMProxy) {
    if (LOG.isDebugEnabled()) {
      if (exception.getCause() != null) {
        LOG.debug("RetryProxy: OM {}: {}: {}", currentOMProxy,
            exception.getCause().getClass().getSimpleName(),
            exception.getCause().getMessage());
      } else {
        LOG.debug("RetryProxy: OM {}: {}", currentOMProxy,
            exception.getMessage());
      }
    }

    // Check if maxFailovers is reached. If yes, return FAIL irrespective of
    // type of exception.
    if (failovers >= maxFailovers) {
      return RetryAction.EXHAUSTED_MAX_FAILOVER_ATTEMPTS;
    }

    ExceptionType exceptionType = getExceptionType(exception);
    RetryAction retryAction = null;
    if (exceptionType != null) {
      retryAction = retryRulesMapByType.get(exceptionType);
      if (retryAction == null) {
        retryAction = defaultRetryRulesMap.get(exceptionType);
      }
    }

    if (retryAction == null) {
      retryAction = retryRulesMapByClass.get(exception.getClass());
    }

    if (retryAction != null) {
      return retryAction;
    }

    return RetryAction.UNDETERMINED;
  }

  /**
   * Get the ExceptioType for the given Exception.
   */
  private ExceptionType getExceptionType(Exception exception) {
    if (exception instanceof ServiceException) {
      Throwable cause = exception.getCause();
      if (cause instanceof RemoteException) {
        IOException ioException =
            ((RemoteException) cause).unwrapRemoteException();
        if (ioException instanceof OMNotLeaderException) {
          return ExceptionType.NOT_LEADER;
        } else if (ioException instanceof OMLeaderNotReadyException) {
          return ExceptionType.LEADER_NOT_READY;
        } else if(ioException instanceof ReconfigurationInProgressException) {
          return ExceptionType.RECONFIGURATION_IN_PROGRESS;
        } else {
          return null;
        }
      }
    }
    return null;
  }
}
