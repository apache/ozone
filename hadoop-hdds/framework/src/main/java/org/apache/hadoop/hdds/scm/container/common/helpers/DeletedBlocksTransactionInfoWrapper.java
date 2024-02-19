package org.apache.hadoop.hdds.scm.container.common.helpers;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DeletedBlocksTransactionInfo;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import java.util.List;

/**
 * The wrapper for {@link DeletedBlocksTransactionInfo}.
 */
public class DeletedBlocksTransactionInfoWrapper {

  private final long txID;
  private final long containerID;
  private final List<Long> localIdList;
  private final int count;

  // Used @JsonCreator to indicate that Jackson should use this constructor for
  // deserialization
  @JsonCreator
  public DeletedBlocksTransactionInfoWrapper(
      @JsonProperty("txID") long txID,
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
