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

package org.apache.hadoop.ozone.protocol.commands;

import java.util.List;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DeleteBlocksCommandProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.SCMCommandProto;

/**
 * A SCM command asks a datanode to delete a number of blocks.
 */
public class DeleteBlocksCommand extends
    SCMCommand<DeleteBlocksCommandProto> {

  private List<DeletedBlocksTransaction> blocksTobeDeleted;

  public DeleteBlocksCommand(List<DeletedBlocksTransaction> blocks) {
    super();
    this.blocksTobeDeleted = blocks;
  }

  // Should be called only for protobuf conversion
  private DeleteBlocksCommand(List<DeletedBlocksTransaction> blocks,
      long id) {
    super(id);
    this.blocksTobeDeleted = blocks;
  }

  public List<DeletedBlocksTransaction> blocksTobeDeleted() {
    return this.blocksTobeDeleted;
  }

  @Override
  public SCMCommandProto.Type getType() {
    return SCMCommandProto.Type.deleteBlocksCommand;
  }

  public static DeleteBlocksCommand getFromProtobuf(
      DeleteBlocksCommandProto deleteBlocksProto) {
    return new DeleteBlocksCommand(deleteBlocksProto
        .getDeletedBlocksTransactionsList(), deleteBlocksProto.getCmdId());
  }

  @Override
  public DeleteBlocksCommandProto getProto() {
    return DeleteBlocksCommandProto.newBuilder()
        .setCmdId(getId())
        .addAllDeletedBlocksTransactions(blocksTobeDeleted).build();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(getType())
        .append(": cmdID: ").append(getId())
        .append(", encodedToken: \"").append(getEncodedToken()).append('"')
        .append(", term: ").append(getTerm())
        .append(", deadlineMsSinceEpoch: ").append(getDeadline())
        .append(", deletedBlocksTransaction: [");
    for (DeletedBlocksTransaction txn : blocksTobeDeleted) {
      sb.append("{ txnID: ").append(txn.getTxID())
          .append(", containerID: ").append(txn.getContainerID())
          .append(", deleteBlockCount: ").append(txn.getLocalIDCount())
          .append(", count: ").append(txn.getCount())
          .append("}, ");
    }
    if (!blocksTobeDeleted.isEmpty()) {
      sb.delete(sb.length() - 2, sb.length());
    }
    sb.append(']');
    return sb.toString();
  }
}
