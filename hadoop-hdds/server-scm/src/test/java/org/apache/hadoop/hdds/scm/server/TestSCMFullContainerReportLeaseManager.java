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

package org.apache.hadoop.hdds.scm.server;

import static org.apache.hadoop.hdds.protocol.MockDatanodeDetails.randomDatanodeDetails;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.junit.jupiter.api.Test;

class TestSCMFullContainerReportLeaseManager {

  @Test
  void shouldLimitOutstandingLeasesAndReleaseAfterReport() {
    AtomicLong now = new AtomicLong(1_000L);
    SCMFullContainerReportLeaseManager leaseManager =
        new SCMFullContainerReportLeaseManager(1, 5_000L, now::get, null);
    DatanodeDetails firstDn = randomDatanodeDetails();
    DatanodeDetails secondDn = randomDatanodeDetails();

    long firstLease = leaseManager.requestLease(firstDn, 7L);
    long secondLease = leaseManager.requestLease(secondDn, 7L);

    assertThat(firstLease).isNotZero();
    assertThat(secondLease).isZero();
    assertThat(leaseManager.getOutstandingLeaseCount()).isEqualTo(1);

    assertThat(leaseManager.checkLease(firstDn, 7L, firstLease)).isTrue();
    leaseManager.removeLease(firstDn);

    long secondLeaseAfterRelease = leaseManager.requestLease(secondDn, 7L);
    assertThat(secondLeaseAfterRelease).isNotZero();
    assertThat(leaseManager.getOutstandingLeaseCount()).isEqualTo(1);
  }

  @Test
  void shouldRejectExpiredAndWrongTermLeases() {
    AtomicLong now = new AtomicLong(1_000L);
    SCMFullContainerReportLeaseManager leaseManager =
        new SCMFullContainerReportLeaseManager(2, 100L, now::get, null);
    DatanodeDetails datanode = randomDatanodeDetails();

    long leaseId = leaseManager.requestLease(datanode, 3L);

    assertThat(leaseManager.checkLease(datanode, 4L, leaseId)).isFalse();

    now.addAndGet(101L);
    assertThat(leaseManager.checkLease(datanode, 3L, leaseId)).isFalse();
    assertThat(leaseManager.getOutstandingLeaseCount()).isZero();
  }

  @Test
  void shouldInvalidateOldTermLeasesWhenGrantingNewTerm() {
    AtomicLong now = new AtomicLong(1_000L);
    SCMFullContainerReportLeaseManager leaseManager =
        new SCMFullContainerReportLeaseManager(1, 5_000L, now::get, null);
    DatanodeDetails firstDn = randomDatanodeDetails();
    DatanodeDetails secondDn = randomDatanodeDetails();

    long oldTermLease = leaseManager.requestLease(firstDn, 3L);
    long newTermLease = leaseManager.requestLease(secondDn, 4L);

    assertThat(oldTermLease).isNotZero();
    assertThat(newTermLease).isNotZero();
    assertThat(leaseManager.checkLease(firstDn, 3L, oldTermLease)).isFalse();
    assertThat(leaseManager.checkLease(secondDn, 4L, newTermLease)).isTrue();
  }
}
