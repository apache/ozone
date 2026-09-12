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

import static org.apache.hadoop.hdds.scm.container.ContainerReplicaChecksumMismatch.hasMismatch;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class TestContainerReplicaChecksumMismatch {

  @Test
  void detectsDifferentChecksumsAtTheSameSequenceId() {
    List<Replica> replicas = Arrays.asList(
        new Replica(10L, 100L),
        new Replica(10L, 200L),
        new Replica(10L, 100L));

    assertTrue(hasMismatch(replicas, Replica::getSequenceId,
        Replica::getDataChecksum));
  }

  @Test
  void ignoresDifferentChecksumsAtDifferentSequenceIds() {
    List<Replica> replicas = Arrays.asList(
        new Replica(10L, 100L),
        new Replica(11L, 200L));

    assertFalse(hasMismatch(replicas, Replica::getSequenceId,
        Replica::getDataChecksum));
  }

  @Test
  void ignoresEqualChecksumsAtDifferentSequenceIds() {
    List<Replica> replicas = Arrays.asList(
        new Replica(10L, 100L),
        new Replica(11L, 100L));

    assertFalse(hasMismatch(replicas, Replica::getSequenceId,
        Replica::getDataChecksum));
  }

  @Test
  void detectsMismatchWithinOneSequenceIdGroup() {
    List<Replica> replicas = Arrays.asList(
        new Replica(10L, 100L),
        new Replica(11L, 200L),
        new Replica(11L, 300L));

    assertTrue(hasMismatch(replicas, Replica::getSequenceId,
        Replica::getDataChecksum));
  }

  @Test
  void waitsUntilEveryReplicaReportsADataChecksum() {
    List<Replica> replicas = Arrays.asList(
        new Replica(10L, 100L),
        new Replica(10L, 200L),
        new Replica(10L, 0L));

    assertFalse(hasMismatch(replicas, Replica::getSequenceId,
        Replica::getDataChecksum));
  }

  @Test
  void waitsUntilEveryReplicaReportsASequenceId() {
    List<Replica> replicas = Arrays.asList(
        new Replica(10L, 100L),
        new Replica(null, 200L));

    assertFalse(hasMismatch(replicas, Replica::getSequenceId,
        Replica::getDataChecksum));
  }

  @Test
  void requiresAtLeastTwoReplicas() {
    assertFalse(hasMismatch(Collections.singletonList(new Replica(10L, 100L)),
        Replica::getSequenceId, Replica::getDataChecksum));
  }

  private static final class Replica {
    private final Long sequenceId;
    private final long dataChecksum;

    private Replica(Long sequenceId, long dataChecksum) {
      this.sequenceId = sequenceId;
      this.dataChecksum = dataChecksum;
    }

    private Long getSequenceId() {
      return sequenceId;
    }

    private long getDataChecksum() {
      return dataChecksum;
    }
  }
}
