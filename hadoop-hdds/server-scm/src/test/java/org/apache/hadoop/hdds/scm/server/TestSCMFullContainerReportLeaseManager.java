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

    SCMFullContainerReportLeaseManager.LeaseClaim claim =
        leaseManager.claimLease(firstDn, 7L, firstLease);
    assertThat(claim).isNotNull();
    assertThat(leaseManager.claimLease(firstDn, 7L, firstLease)).isNull();
    assertThat(leaseManager.getOutstandingLeaseCount()).isEqualTo(1);
    assertThat(leaseManager.requestLease(secondDn, 7L)).isZero();

    leaseManager.completeLease(firstDn, firstLease, true);

    long secondLeaseAfterRelease = leaseManager.requestLease(secondDn, 7L);
    assertThat(secondLeaseAfterRelease).isNotZero();
    assertThat(leaseManager.getOutstandingLeaseCount()).isEqualTo(1);
  }

  @Test
  void shouldRejectExpiredAndWrongTermLeases() {
    AtomicLong now = new AtomicLong(1_000L);
    SCMFullContainerReportLeaseManager leaseManager =
        new SCMFullContainerReportLeaseManager(2, 100L, now::get, null);
    DatanodeDetails wrongTermDatanode = randomDatanodeDetails();
    DatanodeDetails expiredDatanode = randomDatanodeDetails();

    long wrongTermLease = leaseManager.requestLease(wrongTermDatanode, 3L);
    long expiredLease = leaseManager.requestLease(expiredDatanode, 3L);

    assertThat(leaseManager.claimLease(wrongTermDatanode, 4L,
        wrongTermLease)).isNull();

    now.addAndGet(101L);
    assertThat(leaseManager.claimLease(expiredDatanode, 3L,
        expiredLease)).isNull();
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
    assertThat(leaseManager.claimLease(firstDn, 3L, oldTermLease)).isNull();
    assertThat(leaseManager.claimLease(secondDn, 4L, newTermLease)).isNotNull();
  }

  @Test
  void shouldPreserveDeferredRegistrationUntilReportIsProcessed() {
    AtomicLong now = new AtomicLong(1_000L);
    SCMFullContainerReportLeaseManager leaseManager =
        new SCMFullContainerReportLeaseManager(1, 5_000L, now::get, null);
    DatanodeDetails datanode = randomDatanodeDetails();

    leaseManager.markFullContainerReportDeferred(datanode);
    long firstLease = leaseManager.requestLease(datanode, 7L);
    SCMFullContainerReportLeaseManager.LeaseClaim firstClaim =
        leaseManager.claimLease(datanode, 7L, firstLease);

    assertThat(firstClaim).isNotNull();
    assertThat(firstClaim.isRegistrationReport()).isTrue();

    leaseManager.completeLease(datanode, firstLease, false);
    long retryLease = leaseManager.requestLease(datanode, 7L);
    SCMFullContainerReportLeaseManager.LeaseClaim retryClaim =
        leaseManager.claimLease(datanode, 7L, retryLease);

    assertThat(retryClaim).isNotNull();
    assertThat(retryClaim.isRegistrationReport()).isTrue();

    leaseManager.completeLease(datanode, retryLease, true);
    long laterLease = leaseManager.requestLease(datanode, 7L);
    SCMFullContainerReportLeaseManager.LeaseClaim laterClaim =
        leaseManager.claimLease(datanode, 7L, laterLease);

    assertThat(laterClaim).isNotNull();
    assertThat(laterClaim.isRegistrationReport()).isFalse();
  }
}
