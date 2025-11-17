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

package org.apache.hadoop.hdds.scm.container.common.helpers;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DeletedBlocksTransactionInfo;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;

/**
 * The wrapper for {@link DeletedBlocksTransactionInfo}.
 */
public class DeletedBlocksTransactionInfoWrapper {

  private final long txID;
  private final long containerID;
  private final List<Long> localIdList;
  private final int count;

  @JsonCreator
  public DeletedBlocksTransactionInfoWrapper(@JsonProperty("txID") long txID,
                                             @JsonProperty("containerID") long containerID,
                                             @JsonProperty("localIdList") List<Long> localIdList,
                                             @JsonProperty("count") int count) {
    this.txID = txID;
    this.containerID = containerID;
    this.localIdList = localIdList;
    this.count = count;
  }

  public static DeletedBlocksTransactionInfoWrapper fromProtobuf(
      DeletedBlocksTransactionInfo txn) {
    if (txn.hasTxID() && txn.hasContainerID() && txn.hasCount()) {
      return new DeletedBlocksTransactionInfoWrapper(
          txn.getTxID(),
          txn.getContainerID(),
          txn.getLocalIDList(),
          txn.getCount());
    }
    return null;
  }

  public static DeletedBlocksTransactionInfo toProtobuf(
      DeletedBlocksTransactionInfoWrapper wrapper) {
    return DeletedBlocksTransactionInfo.newBuilder()
        .setTxID(wrapper.txID)
        .setContainerID(wrapper.containerID)
        .addAllLocalID(wrapper.localIdList)
        .setCount(wrapper.count)
        .build();
  }

  public static DeletedBlocksTransactionInfo fromTxn(
      DeletedBlocksTransaction txn) {
    return DeletedBlocksTransactionInfo.newBuilder()
        .setTxID(txn.getTxID())
        .setContainerID(txn.getContainerID())
        .addAllLocalID(txn.getLocalIDList())
        .setCount(txn.getCount())
        .build();
  }

  public static DeletedBlocksTransaction toTxn(
      DeletedBlocksTransactionInfo info) {
    return DeletedBlocksTransaction.newBuilder()
        .setTxID(info.getTxID())
        .setContainerID(info.getContainerID())
        .addAllLocalID(info.getLocalIDList())
        .setCount(info.getCount())
        .build();
  }

  public long getTxID() {
    return txID;
  }

  public long getContainerID() {
    return containerID;
  }

  public List<Long> getLocalIdList() {
    return localIdList;
  }

  public int getCount() {
    return count;
  }

  @Override
  public String toString() {
    return "DeletedBlocksTransactionInfoWrapper{" +
        "txID=" + txID +
        ", containerID=" + containerID +
        ", localIdList=" + localIdList +
        ", count=" + count +
        '}';
  }
}
