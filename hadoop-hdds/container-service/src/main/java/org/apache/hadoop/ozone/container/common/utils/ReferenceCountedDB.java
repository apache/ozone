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

package org.apache.hadoop.ozone.container.common.utils;

import com.google.common.base.Preconditions;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.ozone.container.common.interfaces.DBHandle;
import org.apache.hadoop.ozone.container.metadata.DatanodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to implement reference counting over instances handed by Container
 * Cache.
 * Enable DEBUG log below will enable us quickly locate the leaked reference
 * from caller stack. When JDK9 StackWalker is available, we can switch to
 * StackWalker instead of new Exception().printStackTrace().
 */
public class ReferenceCountedDB extends DBHandle {
  private static final Logger LOG =
      LoggerFactory.getLogger(ReferenceCountedDB.class);
  private final AtomicInteger referenceCount;

  public ReferenceCountedDB(DatanodeStore store, String containerDBPath) {
    super(store, containerDBPath);
    this.referenceCount = new AtomicInteger(0);
  }

  public long getReferenceCount() {
    return referenceCount.get();
  }

  public void incrementReference() {
    this.referenceCount.incrementAndGet();
    if (LOG.isTraceEnabled()) {
      LOG.trace("IncRef {} to refCnt {}, stackTrace: {}", getContainerDBPath(),
          referenceCount.get(), ExceptionUtils.getStackTrace(new Throwable()));
    }
  }

  public void decrementReference() {
    int refCount = this.referenceCount.decrementAndGet();
    Preconditions.checkArgument(refCount >= 0, "refCount:", refCount);
    if (LOG.isTraceEnabled()) {
      LOG.trace("DecRef {} to refCnt {}, stackTrace: {}", getContainerDBPath(),
          referenceCount.get(), ExceptionUtils.getStackTrace(new Throwable()));
    }
  }

  @Override
  public boolean cleanup() {
    if (getStore() != null && getStore().isClosed()
        || referenceCount.get() == 0) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Close {} refCnt {}", getContainerDBPath(),
            referenceCount.get());
      }
      getStore().stop();
      return true;
    } else {
      return false;
    }
  }

  @Override
  public void close() {
    decrementReference();
  }

  /**
   * Returns if the underlying DB is closed. This call is threadsafe.
   * @return true if the DB is closed.
   */
  public boolean isClosed() {
    return getStore().isClosed();
  }
}
