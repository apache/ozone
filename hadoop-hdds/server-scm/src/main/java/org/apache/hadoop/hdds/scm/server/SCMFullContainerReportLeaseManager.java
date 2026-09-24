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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.LongSupplier;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.container.metrics.SCMContainerManagerMetrics;

/**
 * Tracks transient leases for full container reports.
 */
public class SCMFullContainerReportLeaseManager {
  private final int maxOutstandingLeases;
  private final long leaseExpiryMs;
  private final LongSupplier clock;
  private final SCMContainerManagerMetrics metrics;
  private final Map<String, Lease> pendingLeases = new HashMap<>();
  private final Map<String, Long> deferredRegistrationReports = new HashMap<>();
  private long nextDeferredRegistrationId;

  public SCMFullContainerReportLeaseManager(int maxOutstandingLeases,
      long leaseExpiryMs, LongSupplier clock, SCMContainerManagerMetrics metrics) {
    this.maxOutstandingLeases = maxOutstandingLeases;
    this.leaseExpiryMs = leaseExpiryMs;
    this.clock = clock;
    this.metrics = metrics;
  }

  public synchronized long requestLease(DatanodeDetails datanode, long term) {
    incrementLeaseRequests();
    pruneExpiredLeases();
    removeLeasesFromOtherTerms(term);
    String datanodeId = datanode.getUuidString();
    Lease existingLease = pendingLeases.get(datanodeId);
    if (existingLease != null && existingLease.claimed) {
      incrementLeasesRejected();
      updateOutstandingLeaseMetric();
      return 0;
    }
    pendingLeases.remove(datanodeId);

    if (pendingLeases.size() >= maxOutstandingLeases) {
      incrementLeasesRejected();
      updateOutstandingLeaseMetric();
      return 0;
    }

    long leaseId = nextLeaseId();
    long deferredRegistrationId =
        deferredRegistrationReports.getOrDefault(datanodeId, 0L);
    pendingLeases.put(datanodeId,
        new Lease(leaseId, term, clock.getAsLong(), deferredRegistrationId));
    incrementLeasesGranted();
    updateOutstandingLeaseMetric();
    return leaseId;
  }

  public synchronized LeaseClaim claimLease(DatanodeDetails datanode, long term,
      long leaseId) {
    if (leaseId == 0) {
      incrementInvalidLeaseReports();
      return null;
    }

    Lease lease = pendingLeases.get(datanode.getUuidString());
    if (lease == null || lease.claimed) {
      incrementInvalidLeaseReports();
      return null;
    }

    if (isExpired(lease)) {
      pendingLeases.remove(datanode.getUuidString());
      incrementLeaseExpired();
      incrementInvalidLeaseReports();
      updateOutstandingLeaseMetric();
      return null;
    }

    if (lease.term != term) {
      if (term > lease.term) {
        pendingLeases.remove(datanode.getUuidString());
        updateOutstandingLeaseMetric();
      }
      incrementInvalidLeaseReports();
      return null;
    }

    if (lease.leaseId != leaseId) {
      incrementInvalidLeaseReports();
      return null;
    }

    lease.claimed = true;
    return new LeaseClaim(lease.deferredRegistrationId != 0);
  }

  public synchronized void completeLease(DatanodeDetails datanode,
      long leaseId, boolean reportProcessed) {
    String datanodeId = datanode.getUuidString();
    Lease lease = pendingLeases.get(datanodeId);
    if (lease == null || lease.leaseId != leaseId) {
      return;
    }
    pendingLeases.remove(datanodeId);
    if (reportProcessed) {
      if (lease.deferredRegistrationId != 0
          && deferredRegistrationReports.getOrDefault(datanodeId, 0L)
          == lease.deferredRegistrationId) {
        deferredRegistrationReports.remove(datanodeId);
      }
      incrementReportsProcessedWithLease();
    }
    updateOutstandingLeaseMetric();
  }

  public synchronized void markFullContainerReportDeferred(
      DatanodeDetails datanode) {
    long deferredRegistrationId = ++nextDeferredRegistrationId;
    if (deferredRegistrationId == 0) {
      deferredRegistrationId = ++nextDeferredRegistrationId;
    }
    String datanodeId = datanode.getUuidString();
    deferredRegistrationReports.put(datanodeId, deferredRegistrationId);
    Lease lease = pendingLeases.get(datanodeId);
    if (lease != null && !lease.claimed) {
      pendingLeases.remove(datanodeId);
      updateOutstandingLeaseMetric();
    }
  }

  public synchronized void clearFullContainerReportDeferred(
      DatanodeDetails datanode) {
    deferredRegistrationReports.remove(datanode.getUuidString());
  }

  public synchronized void recordInvalidLeaseReport() {
    incrementInvalidLeaseReports();
  }

  public synchronized int getOutstandingLeaseCount() {
    pruneExpiredLeases();
    updateOutstandingLeaseMetric();
    return pendingLeases.size();
  }

  private long nextLeaseId() {
    long leaseId = 0;
    while (leaseId == 0) {
      leaseId = ThreadLocalRandom.current().nextLong();
    }
    return leaseId;
  }

  private void pruneExpiredLeases() {
    Iterator<Map.Entry<String, Lease>> iterator = pendingLeases.entrySet()
        .iterator();
    boolean removed = false;
    while (iterator.hasNext()) {
      Lease lease = iterator.next().getValue();
      if (!lease.claimed && isExpired(lease)) {
        iterator.remove();
        incrementLeaseExpired();
        removed = true;
      }
    }
    if (removed) {
      updateOutstandingLeaseMetric();
    }
  }

  private void removeLeasesFromOtherTerms(long term) {
    Iterator<Map.Entry<String, Lease>> iterator = pendingLeases.entrySet()
        .iterator();
    boolean removed = false;
    while (iterator.hasNext()) {
      Lease lease = iterator.next().getValue();
      if (!lease.claimed && lease.term != term) {
        iterator.remove();
        removed = true;
      }
    }
    if (removed) {
      updateOutstandingLeaseMetric();
    }
  }

  private boolean isExpired(Lease lease) {
    return clock.getAsLong() - lease.createdAtMs >= leaseExpiryMs;
  }

  private void incrementLeaseRequests() {
    if (metrics != null) {
      metrics.incNumFCRLeaseRequests();
    }
  }

  private void incrementLeasesGranted() {
    if (metrics != null) {
      metrics.incNumFCRLeasesGranted();
    }
  }

  private void incrementLeasesRejected() {
    if (metrics != null) {
      metrics.incNumFCRLeasesRejected();
    }
  }

  private void incrementLeaseExpired() {
    if (metrics != null) {
      metrics.incNumFCRLeaseExpired();
    }
  }

  private void incrementInvalidLeaseReports() {
    if (metrics != null) {
      metrics.incNumFCRReportsRejectedInvalidLease();
    }
  }

  private void incrementReportsProcessedWithLease() {
    if (metrics != null) {
      metrics.incNumFCRReportsProcessedWithLease();
    }
  }

  private void updateOutstandingLeaseMetric() {
    if (metrics != null) {
      metrics.setNumFCRLeasesOutstanding(pendingLeases.size());
    }
  }

  private static final class Lease {
    private final long leaseId;
    private final long term;
    private final long createdAtMs;
    private final long deferredRegistrationId;
    private boolean claimed;

    private Lease(long leaseId, long term, long createdAtMs,
        long deferredRegistrationId) {
      this.leaseId = leaseId;
      this.term = term;
      this.createdAtMs = createdAtMs;
      this.deferredRegistrationId = deferredRegistrationId;
    }
  }

  static final class LeaseClaim {
    private final boolean registrationReport;

    private LeaseClaim(boolean registrationReport) {
      this.registrationReport = registrationReport;
    }

    boolean isRegistrationReport() {
      return registrationReport;
    }
  }
}
