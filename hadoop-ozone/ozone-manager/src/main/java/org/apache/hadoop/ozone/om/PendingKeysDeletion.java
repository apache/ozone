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

package org.apache.hadoop.ozone.om;

import java.util.Map;
import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;

/**
 * Tracks metadata for keys pending deletion and their associated blocks.
 *
 * This class maintains:
 * <ul>
 *   <li>A list of {@link BlockGroup} entries, where each entry contains
 *       a key name and its associated block IDs</li>
 *   <li>A key-value mapping that requires updating after the remaining
 *       blocks are purged</li>
 * </ul>
 */
public class PendingKeysDeletion {

  private final Map<String, RepeatedOmKeyInfo> keysToModify;
  private final Map<String, PurgedKey> purgedKeys;
  private int notReclaimableKeyCount;

  public PendingKeysDeletion(Map<String, PurgedKey> purgedKeys,
      Map<String, RepeatedOmKeyInfo> keysToModify,
      int notReclaimableKeyCount) {
    this.keysToModify = keysToModify;
    this.purgedKeys = purgedKeys;
    this.notReclaimableKeyCount = notReclaimableKeyCount;
  }

  public Map<String, RepeatedOmKeyInfo> getKeysToModify() {
    return keysToModify;
  }

  public Map<String, PurgedKey> getPurgedKeys() {
    return purgedKeys;
  }

  /**
   * Represents metadata for a key that has been purged.
   *
   * This class holds information about a specific purged key,
   * including its volume, bucket, associated block group,
   * and the amount of data purged in bytes.
   */
  public static class PurgedKey {
    private final String volume;
    private final String bucket;
    private final long bucketId;
    private final BlockGroup blockGroup;
    private final long purgedBytes;
    private final boolean isCommittedKey;
    private final String deleteKeyName;

    public PurgedKey(String volume, String bucket, long bucketId, BlockGroup group, String deleteKeyName,
        long purgedBytes, boolean isCommittedKey) {
      this.volume = volume;
      this.bucket = bucket;
      this.bucketId = bucketId;
      this.blockGroup = group;
      this.purgedBytes = purgedBytes;
      this.isCommittedKey = isCommittedKey;
      this.deleteKeyName = deleteKeyName;
    }

    public BlockGroup getBlockGroup() {
      return blockGroup;
    }

    public long getPurgedBytes() {
      return purgedBytes;
    }

    public String getVolume() {
      return volume;
    }

    public String getBucket() {
      return bucket;
    }

    public long getBucketId() {
      return bucketId;
    }

    public boolean isCommittedKey() {
      return isCommittedKey;
    }

    public String getDeleteKeyName() {
      return deleteKeyName;
    }

    @Override
    public String toString() {
      return "PurgedKey{" +
          "blockGroup=" + blockGroup +
          ", volume='" + volume + '\'' +
          ", bucket='" + bucket + '\'' +
          ", bucketId=" + bucketId +
          ", purgedBytes=" + purgedBytes +
          ", isCommittedKey=" + isCommittedKey +
          ", deleteKeyName='" + deleteKeyName + '\'' +
          '}';
    }
  }

  public int getNotReclaimableKeyCount() {
    return notReclaimableKeyCount;
  }
}
