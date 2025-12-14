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

package org.apache.hadoop.hdds.scm.storage;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Map;
import java.util.Objects;
import org.apache.hadoop.hdds.scm.client.HddsClientUtils;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.ozone.client.io.ByteBufferOutputStream;
import org.apache.ratis.protocol.exceptions.AlreadyClosedException;
import org.apache.ratis.protocol.exceptions.RaftRetryFailureException;

/**
 * This class is used for error handling methods.
 */
public abstract class AbstractDataStreamOutput
    extends ByteBufferOutputStream {

  private final Map<Class<? extends Throwable>, RetryPolicy> retryPolicyMap;
  private int retryCount;
  private boolean isException;

  protected AbstractDataStreamOutput(
      Map<Class<? extends Throwable>, RetryPolicy> retryPolicyMap) {
    this.retryPolicyMap = retryPolicyMap;
    this.isException = false;
    this.retryCount = 0;
  }

  protected void resetRetryCount() {
    retryCount = 0;
  }

  protected boolean isException() {
    return isException;
  }

  /**
   * Checks if the provided exception signifies retry failure in ratis client.
   * In case of retry failure, ratis client throws RaftRetryFailureException
   * and all succeeding operations are failed with AlreadyClosedException.
   */
  protected boolean checkForRetryFailure(Throwable t) {
    return t instanceof RaftRetryFailureException
        || t instanceof AlreadyClosedException;
  }

  // Every container specific exception from datatnode will be seen as
  // StorageContainerException
  protected boolean checkIfContainerToExclude(Throwable t) {
    return t instanceof StorageContainerException;
  }

  protected void setExceptionAndThrow(IOException ioe) throws IOException {
    isException = true;
    throw ioe;
  }

  protected void handleRetry(IOException exception) throws IOException {
    RetryPolicy retryPolicy = retryPolicyMap
        .get(HddsClientUtils.checkForException(exception).getClass());
    if (retryPolicy == null) {
      retryPolicy = retryPolicyMap.get(Exception.class);
    }
    handleRetry(exception, retryPolicy);
  }

  protected void handleRetry(IOException exception, RetryPolicy retryPolicy)
      throws IOException {
    RetryPolicy.RetryAction action = null;
    try {
      action = retryPolicy.shouldRetry(exception, retryCount, 0, true);
    } catch (Exception e) {
      setExceptionAndThrow(new IOException(e));
    }
    if (action != null &&
        action.action == RetryPolicy.RetryAction.RetryDecision.FAIL) {
      String msg = "";
      if (action.reason != null) {
        msg = "Retry request failed. " + action.reason;
      }
      setExceptionAndThrow(new IOException(msg, exception));
    }

    // Throw the exception if the thread is interrupted
    if (Thread.currentThread().isInterrupted()) {
      setExceptionAndThrow(exception);
    }
    Objects.requireNonNull(action, "action == null");
    Preconditions.checkArgument(
        action.action == RetryPolicy.RetryAction.RetryDecision.RETRY);
    if (action.delayMillis > 0) {
      try {
        Thread.sleep(action.delayMillis);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        IOException ioe = (IOException) new InterruptedIOException(
            "Interrupted: action=" + action + ", retry policy=" + retryPolicy)
            .initCause(e);
        setExceptionAndThrow(ioe);
      }
    }
    retryCount++;
  }
}
