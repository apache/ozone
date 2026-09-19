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

package org.apache.hadoop.hdds.scm.container;

import java.util.Collection;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.ToLongFunction;

/**
 * Detects different data checksums reported for the same container replica
 * sequence ID.
 */
public final class ContainerReplicaChecksumMismatch {

  private ContainerReplicaChecksumMismatch() {
  }

  /**
   * Returns true when replicas with the same sequence ID report different
   * non-zero data checksums. If any replica has not reported a sequence ID or
   * checksum yet, no comparison is made.
   */
  public static <T> boolean hasMismatch(Collection<T> replicas,
      Function<T, Long> sequenceId, ToLongFunction<T> dataChecksum) {
    Objects.requireNonNull(sequenceId, "sequenceId == null");
    Objects.requireNonNull(dataChecksum, "dataChecksum == null");

    if (replicas == null || replicas.size() < 2) {
      return false;
    }

    for (T replica : replicas) {
      Long replicaSequenceId = sequenceId.apply(replica);
      long replicaDataChecksum = dataChecksum.applyAsLong(replica);
      if (replicaSequenceId == null || replicaDataChecksum == 0) {
        return false;
      }
    }

    for (T left : replicas) {
      Long leftSequenceId = sequenceId.apply(left);
      long leftDataChecksum = dataChecksum.applyAsLong(left);
      for (T right : replicas) {
        if (leftSequenceId.equals(sequenceId.apply(right)) &&
            leftDataChecksum != dataChecksum.applyAsLong(right)) {
          return true;
        }
      }
    }
    return false;
  }
}
