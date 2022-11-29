package org.apache.hadoop.ozone.om;

import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.util.Time;

/**
 * Keys in deletion table are hex-encoded String of Timestamp + UpdateID.
 * The idea is to guarantee rough ordering of deletion by timestamp, and
 * to guarantee the uniqueness by UpdateID (transaction index).
 *
 * UpdateID is transaction index from Ratis, assuming it is already set
 * during the OMKeyRequest. The transaction index may not be consistently
 * increasing across OM restart, or Ratis configuration change.
 *
 * To address the ordering inconsistency, it is prefixed with hex-encoded
 * timestamp obtained by Time.now(). Its precision depends on the system -
 * mostly around 10 milliseconds. We only need rough ordering of actual
 * deletion of deleted keys - based on the order of deletion called by
 * clients. We want older deleted keys being processed earlier by deletion
 * service, and newer keys being processed later.
 *
 * Caveat: the only change where it loses the uniqueness is when the system
 * clock issues the same two timestamps for JVM across OM restart. Say,
 * before restart, in time t1 from Time.now() with transaction index 1
 * "t1-0001" issued on deletion, and OM restart happens, and t2 from
 * Time.now() with another transaction index 1 "t2-0001" issued on deletion
 * with transaction index being reset.
 * Although t1!=t2 is not guaranteed by the system, we can (almost) safely
 * assume OM restart takes long enough so that t1==t2 almost never happens.
 **/
public class DeleteTablePrefix {
  // Will be zero when Ratis is enabled.
  private long timestamp;
  private long transactionLogIndex;

  public DeleteTablePrefix(long transactionLogIndex, boolean isRatisEnabled) {
    // Rule out default value of protocol buffers
    if (isRatisEnabled) {
      this.timestamp = 0;
    } else {
      this.timestamp = Time.now();
    }
    this.transactionLogIndex = transactionLogIndex;
  }

  /**
   * Build the key in delete table as string to put into delete table.
   *
   * @param omKeyInfo the key info to get object id from
   * @return Unique and monotonically increasing String for deletion table
   */
  public String buildKey(OmKeyInfo omKeyInfo) {
    // Log.toHexString() is much faster, but the string is compact. Heading 0s
    // are all erased. e.g. 15L becomes "F", while we want "00000000000F".
    if (timestamp == 0) {
      // object id and update id are both needed to identify different updates.
      return String.format("%016X-%X-%X", transactionLogIndex,
              omKeyInfo.getObjectID(), omKeyInfo.getUpdateID());
    }
    // When Ratis is enabled, we need timestamp as transaction log may be
    // reset. It is appended after the transaction log index to keep the
    // ordering across Ratis turned on and off. Keys deleted during non-HA era
    // be deleted before keys deleted during HA period, because all systems
    // will finally be upgraded to HA-enabled configuration even though the
    // system has single OM.
    return String.format("%016X-%016X-%X-%X", transactionLogIndex, timestamp,
            omKeyInfo.getObjectID(), omKeyInfo.getUpdateID());
  }
}
