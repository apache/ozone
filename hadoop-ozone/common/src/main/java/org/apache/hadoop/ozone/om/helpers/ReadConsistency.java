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

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ReadConsistencyProto;

/**
 * Supported read consistency.
 */
public enum ReadConsistency {
  DEFAULT(false, false),
  LOCAL_LEASE(false, true),
  LINEARIZABLE_LEADER_ONLY(true, false),
  LINEARIZABLE_ALLOW_FOLLOWER(true, true);

  private final boolean linearizable;
  private final boolean followerRead;

  ReadConsistency(boolean linearizable, boolean followerRead) {
    this.linearizable = linearizable;
    this.followerRead = followerRead;
  }

  public boolean isLinearizable() {
    return linearizable;
  }

  public boolean allowFollowerRead() {
    return followerRead;
  }

  public ReadConsistencyProto toProto() {
    switch (this) {
    case DEFAULT:
      return ReadConsistencyProto.DEFAULT;
    case LINEARIZABLE_LEADER_ONLY:
      return ReadConsistencyProto.LINEARIZABLE_LEADER_ONLY;
    case LINEARIZABLE_ALLOW_FOLLOWER:
      return ReadConsistencyProto.LINEARIZABLE_ALLOW_FOLLOWER;
    case LOCAL_LEASE:
      return ReadConsistencyProto.LOCAL_LEASE;
    default:
      return ReadConsistencyProto.UNSPECIFIED;
    }
  }

  public static ReadConsistency fromProto(ReadConsistencyProto readConsistencyTypeProto) {
    if (readConsistencyTypeProto == null) {
      return ReadConsistency.DEFAULT;
    }
    switch (readConsistencyTypeProto) {
    case LINEARIZABLE_LEADER_ONLY:
      return ReadConsistency.LINEARIZABLE_LEADER_ONLY;
    case LINEARIZABLE_ALLOW_FOLLOWER:
      return ReadConsistency.LINEARIZABLE_ALLOW_FOLLOWER;
    case LOCAL_LEASE:
      return ReadConsistency.LOCAL_LEASE;
    case DEFAULT:
    default:
      return ReadConsistency.DEFAULT;
    }
  }
}
