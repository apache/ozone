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

package org.apache.hadoop.ozone.om.execution;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.om.OMMetadataManager;

/**
 * Manages a monotonically increasing index decoupled from Ratis transaction
 * index. Used for object ID generation in the planned execution path.
 *
 * <p>The index survives leader switchover via:
 * {@code currentIndex = max(currentIndex, lastCommittedIndex)}
 */
public final class ManagedIndexService {

  private static final String MANAGED_INDEX_KEY = "#MANAGED_INDEX";

  private final AtomicLong currentIndex;

  public ManagedIndexService(long initialIndex) {
    this.currentIndex = new AtomicLong(initialIndex);
  }

  public long getAndIncrement() {
    return currentIndex.incrementAndGet();
  }

  public long current() {
    return currentIndex.get();
  }

  public void updateCommitIndex(long committedIndex) {
    currentIndex.updateAndGet(current -> Math.max(current, committedIndex));
  }

  public void onBecomeLeader(long lastCommittedIndex) {
    currentIndex.updateAndGet(current -> Math.max(current, lastCommittedIndex));
  }

  public void persistWithBatch(OMMetadataManager metadataManager,
      BatchOperation batch, long managedIndex) throws IOException {
    metadataManager.getTransactionInfoTable().putWithBatch(
        batch, MANAGED_INDEX_KEY,
        org.apache.hadoop.hdds.utils.TransactionInfo.valueOf(0, managedIndex));
  }

  public static ManagedIndexService recover(OMMetadataManager metadataManager)
      throws IOException {
    org.apache.hadoop.hdds.utils.TransactionInfo info =
        metadataManager.getTransactionInfoTable().get(MANAGED_INDEX_KEY);
    long recovered = (info != null) ? info.getTransactionIndex() : 0;
    return new ManagedIndexService(recovered);
  }
}
