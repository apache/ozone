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

package org.apache.hadoop.ozone.om.ratis;

import static org.apache.hadoop.ozone.OzoneConsts.TRANSACTION_INFO_KEY;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.execution.ManagedIndexService;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BatchedStateTransitions;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DBDelta;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeltaType;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ReplicatedStateTransition;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.ratis.server.protocol.TermIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Applies {@link ReplicatedStateTransition} payloads as atomic RocksDB
 * batch writes. This is the apply path for all nodes — writes raw bytes
 * directly to column families without domain-object deserialization.
 */
public final class StateTransitionApplyEngine {

  private static final Logger LOG = LoggerFactory.getLogger(StateTransitionApplyEngine.class);

  private final OMMetadataManager metadataManager;
  private final ManagedIndexService managedIndexService;

  public StateTransitionApplyEngine(OMMetadataManager metadataManager,
      ManagedIndexService managedIndexService) {
    this.metadataManager = metadataManager;
    this.managedIndexService = managedIndexService;
  }

  public void apply(ReplicatedStateTransition transition, TermIndex termIndex) throws IOException {
    try (BatchOperation batch = metadataManager.getStore().initBatchOperation()) {
      applyDeltas(transition, batch);
      writeTransactionInfo(batch, termIndex);
      writeManagedIndex(batch, transition.getManagedIndex());
      metadataManager.getStore().commitBatchOperation(batch);
    }
    managedIndexService.updateCommitIndex(transition.getManagedIndex());
  }

  public void applyBatch(BatchedStateTransitions batched, TermIndex termIndex) throws IOException {
    List<Queue<ReplicatedStateTransition>> splits = splitAtBarriers(batched);
    for (Queue<ReplicatedStateTransition> segment : splits) {
      applySegment(segment, termIndex);
    }
  }

  private void applySegment(Queue<ReplicatedStateTransition> segment,
      TermIndex termIndex) throws IOException {
    long maxManagedIndex = 0;
    try (BatchOperation batch = metadataManager.getStore().initBatchOperation()) {
      for (ReplicatedStateTransition transition : segment) {
        applyDeltas(transition, batch);
        maxManagedIndex = Math.max(maxManagedIndex, transition.getManagedIndex());
      }
      writeTransactionInfo(batch, termIndex);
      writeManagedIndex(batch, maxManagedIndex);
      metadataManager.getStore().commitBatchOperation(batch);
    }
    managedIndexService.updateCommitIndex(maxManagedIndex);
  }

  @SuppressWarnings("unchecked")
  private void applyDeltas(ReplicatedStateTransition transition,
      BatchOperation batch) throws IOException {
    for (DBDelta delta : transition.getDeltasList()) {
      Table<byte[], byte[]> table = metadataManager.getStore().getTable(delta.getTableName());
      byte[] key = delta.getKey().toByteArray();
      if (delta.getType() == DeltaType.PUT) {
        byte[] value = delta.getValue().toByteArray();
        table.putWithBatch(batch, key, value);
      } else if (delta.getType() == DeltaType.DELETE) {
        table.deleteWithBatch(batch, key);
      }
    }
  }

  private void writeTransactionInfo(BatchOperation batch, TermIndex termIndex) throws IOException {
    metadataManager.getTransactionInfoTable().putWithBatch(
        batch, TRANSACTION_INFO_KEY, TransactionInfo.valueOf(termIndex));
  }

  private void writeManagedIndex(BatchOperation batch, long managedIndex) throws IOException {
    managedIndexService.persistWithBatch(metadataManager, batch, managedIndex);
  }

  static List<Queue<ReplicatedStateTransition>> splitAtBarriers(
      BatchedStateTransitions batched) {
    List<Queue<ReplicatedStateTransition>> result = new ArrayList<>();
    Type previousType = null;

    for (ReplicatedStateTransition transition : batched.getTransitionsList()) {
      Type currentType = transition.getCmdType();
      if (result.isEmpty()
          || isSnapshotBarrier(currentType)
          || isSnapshotBarrier(previousType)) {
        result.add(new LinkedList<>());
      }
      result.get(result.size() - 1).add(transition);
      previousType = currentType;
    }
    return result;
  }

  private static boolean isSnapshotBarrier(Type type) {
    return type == Type.CreateSnapshot || type == Type.SnapshotPurge;
  }
}
