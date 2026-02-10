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

package org.apache.hadoop.ozone.om.helpers;

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ReadConsistencyType;

/**
 * Supported read consistency. It consists of two elements
 * <ol>
 *   <li>
 *     {@link ConsistencyType} which specifies different types of read consistency
 *     (e.g. LINEARIZABLE, LOCAL_LEASE)
 *   </li>
 *   <li>
 *     Whether to allow follower read. Some consistency types supports
 *     follower read (e.g. LINEARIZABLE) while other consistency types only
 *     supports leader read (e.g. NON_LINEARIZABLE).
 *   </li>
 * </ol>
 *
 * handles different types of read consistency (e.g. LINEARIZABLE, LOCAL_LEASE)
 */
public enum ReadConsistency {
  DEFAULT(ConsistencyType.NON_LINEARIZABLE, false),
  STALE(ConsistencyType.STALE, true),
  LINEARIZABLE_LEADER_READ(ConsistencyType.LINEARIZABLE, false),
  LINEARIZABLE_FOLLOWER_READ(ConsistencyType.LINEARIZABLE, true),
  LOCAL_LEASE_FOLLOWER_READ(ConsistencyType.LOCAL_LEASE, true);

  private final ConsistencyType consistencyType;
  private final boolean allowFollowerRead;

  /**
   * Supported consistency type.
   */
  public enum ConsistencyType {
    NON_LINEARIZABLE,
    STALE,
    LINEARIZABLE,
    LOCAL_LEASE
  }

  ReadConsistency(ConsistencyType consistencyType, boolean allowFollowerRead) {
    this.consistencyType = consistencyType;
    this.allowFollowerRead = allowFollowerRead;
  }

  public ConsistencyType getConsistencyType() {
    return consistencyType;
  }

  public boolean isAllowFollowerRead() {
    return allowFollowerRead;
  }

  public static ReadConsistency valueOf(String consistencyTypeString, boolean allowFollowerRead) {
    ConsistencyType consistencyType = ConsistencyType.valueOf(consistencyTypeString);
    switch (consistencyType) {
    case LINEARIZABLE:
      return allowFollowerRead ? LINEARIZABLE_FOLLOWER_READ : LINEARIZABLE_LEADER_READ;
    case LOCAL_LEASE:
      if (!allowFollowerRead) {
        throw new IllegalArgumentException("LOCAL_LEASE consistency is not valid for " +
            "reading from leader, use NON_LINEARIZABLE instead");
      }
      return LOCAL_LEASE_FOLLOWER_READ;
    case STALE:
      if (!allowFollowerRead) {
        // Although technically leader read can use STALE since it has the same behavior to NON_LINEARIZABLE,
        // it is better to be explicit and use NON_LINEARIZABLE instead.
        throw new IllegalArgumentException("STALE consistency is not valid for reading from leader, " +
            "use NON_LINEARIZABLE instead");
      }
      return STALE;
    case NON_LINEARIZABLE:
      if (allowFollowerRead) {
        throw new IllegalArgumentException("NON_LINEARIZABLE consistency is not valid for " +
            "reading from followers");
      }
      return DEFAULT;
    default:
      throw new IllegalArgumentException("No ReadConsistency with type=" + consistencyTypeString + " and " +
          "allowFollowerRead=" + allowFollowerRead);
    }
  }

  public ReadConsistencyType toProto() {
    switch (this) {
    case DEFAULT:
      return ReadConsistencyType.DEFAULT;
    case LINEARIZABLE_LEADER_READ:
      return ReadConsistencyType.LINEARIZABLE_LEADER_READ;
    case LINEARIZABLE_FOLLOWER_READ:
      return ReadConsistencyType.LINEARIZABLE_FOLLOWER_READ;
    case LOCAL_LEASE_FOLLOWER_READ:
      return ReadConsistencyType.LOCAL_LEASE_FOLLOWER_READ;
    case STALE:
      return ReadConsistencyType.STALE;
    default:
      return ReadConsistencyType.CONSISTENCY_TYPE_UNKNOWN;
    }
  }

  public static ReadConsistency fromProto(ReadConsistencyType readConsistencyTypeProto) {
    if (readConsistencyTypeProto == null) {
      return ReadConsistency.DEFAULT;
    }
    switch (readConsistencyTypeProto) {
    case LINEARIZABLE_LEADER_READ:
      return ReadConsistency.LINEARIZABLE_LEADER_READ;
    case LINEARIZABLE_FOLLOWER_READ:
      return ReadConsistency.LINEARIZABLE_FOLLOWER_READ;
    case LOCAL_LEASE_FOLLOWER_READ:
      return ReadConsistency.LOCAL_LEASE_FOLLOWER_READ;
    case STALE:
      return ReadConsistency.STALE;
    case DEFAULT:
    default:
      return ReadConsistency.DEFAULT;
    }
  }
}
