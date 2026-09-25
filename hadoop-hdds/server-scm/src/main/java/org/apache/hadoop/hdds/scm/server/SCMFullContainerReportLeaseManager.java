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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.LongSupplier;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;

/**
 * Tracks transient leases for full container reports.
 */
public class SCMFullContainerReportLeaseManager {
  private final int maxOutstandingLeases;
  private final long leaseExpiryMs;
  private final LongSupplier clock;
  private final SCMFullContainerReportLeaseMetrics metrics;
  private final Map<String, Lease> pendingLeases = new HashMap<>();
  private final Map<String, LeaseRequest> pendingRequests =
      new LinkedHashMap<>();
  private final Map<String, Long> deferredRegistrationReports = new HashMap<>();
  private long nextDeferredRegistrationId;

  public SCMFullContainerReportLeaseManager(int maxOutstandingLeases,
      long leaseExpiryMs, LongSupplier clock,
      SCMFullContainerReportLeaseMetrics metrics) {
    if (maxOutstandingLeases < 1) {
      throw new IllegalArgumentException(
          "Maximum outstanding full container report leases must be at least 1");
    }
    if (leaseExpiryMs < 1) {
      throw new IllegalArgumentException(
          "Full container report lease duration must be at least 1 ms");
    }
    this.maxOutstandingLeases = maxOutstandingLeases;
    this.leaseExpiryMs = leaseExpiryMs;
    this.clock = clock;
    this.metrics = metrics;
  }

  public synchronized OptionalLong requestLease(
      DatanodeDetails datanode, long term) {
    incrementLeaseRequests();
    long now = clock.getAsLong();
    pruneExpiredLeases(now);
    removeLeasesFromOtherTerms(term);
    removeRequestsFromOtherTerms(term);
    pruneExpiredRequests(now);
    String datanodeId = datanode.getUuidString();
    Lease existingLease = pendingLeases.get(datanodeId);
    if (existingLease != null && existingLease.claimed) {
      incrementLeasesRejected();
      updateOutstandingLeaseMetric();
      return OptionalLong.empty();
    }
    pendingLeases.remove(datanodeId);

    LeaseRequest request = pendingRequests.get(datanodeId);
    if (request == null) {
      request = new LeaseRequest(term, now);
      pendingRequests.put(datanodeId, request);
    } else {
      request.lastRequestAtMs = now;
    }

    if (pendingLeases.size() >= maxOutstandingLeases
        || !datanodeId.equals(firstPendingRequest())) {
      incrementLeasesRejected();
      updateOutstandingLeaseMetric();
      return OptionalLong.empty();
    }

    pendingRequests.remove(datanodeId);
    long leaseId = nextLeaseId();
    long deferredRegistrationId =
        deferredRegistrationReports.getOrDefault(datanodeId, 0L);
    pendingLeases.put(datanodeId,
        new Lease(leaseId, term, now, deferredRegistrationId));
    incrementLeasesGranted();
    updateOutstandingLeaseMetric();
    return OptionalLong.of(leaseId);
  }

  public synchronized Optional<LeaseClaim> claimLease(
      DatanodeDetails datanode, OptionalLong currentTerm, long leaseTerm,
      long leaseId) {
    if (!currentTerm.isPresent() || currentTerm.getAsLong() != leaseTerm
        || leaseId == 0) {
      incrementInvalidLeaseReports();
      return Optional.empty();
    }

    String datanodeId = datanode.getUuidString();
    Lease lease = pendingLeases.get(datanodeId);
    if (lease == null) {
      incrementInvalidLeaseReports();
      return Optional.empty();
    }

    long now = clock.getAsLong();
    if (isExpired(lease, now)) {
      pendingLeases.remove(datanodeId);
      incrementLeaseExpired();
      incrementInvalidLeaseReports();
      updateOutstandingLeaseMetric();
      return Optional.empty();
    }

    if (lease.claimed) {
      incrementInvalidLeaseReports();
      return Optional.empty();
    }

    if (lease.term != currentTerm.getAsLong()) {
      if (currentTerm.getAsLong() > lease.term) {
        pendingLeases.remove(datanodeId);
        updateOutstandingLeaseMetric();
      }
      incrementInvalidLeaseReports();
      return Optional.empty();
    }

    if (lease.leaseId != leaseId) {
      incrementInvalidLeaseReports();
      return Optional.empty();
    }

    lease.claimed = true;
    lease.claimedAtMs = now;
    return Optional.of(new LeaseClaim(this, datanodeId, leaseId,
        lease.deferredRegistrationId != 0));
  }

  private synchronized void completeLease(String datanodeId, long leaseId,
      boolean reportProcessed) {
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
    pendingRequests.remove(datanodeId);
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

  public synchronized void removeDatanode(DatanodeDetails datanode) {
    String datanodeId = datanode.getUuidString();
    boolean leaseRemoved = pendingLeases.remove(datanodeId) != null;
    pendingRequests.remove(datanodeId);
    deferredRegistrationReports.remove(datanodeId);
    if (leaseRemoved) {
      updateOutstandingLeaseMetric();
    }
  }

  public synchronized int getOutstandingLeaseCount() {
    pruneExpiredLeases(clock.getAsLong());
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

  private void pruneExpiredLeases(long now) {
    Iterator<Map.Entry<String, Lease>> iterator = pendingLeases.entrySet()
        .iterator();
    boolean removed = false;
    while (iterator.hasNext()) {
      Lease lease = iterator.next().getValue();
      if (!lease.processing && isExpired(lease, now)) {
        iterator.remove();
        incrementLeaseExpired();
        removed = true;
      }
    }
    if (removed) {
      updateOutstandingLeaseMetric();
    }
  }

  private void removeRequestsFromOtherTerms(long term) {
    pendingRequests.values().removeIf(request -> request.term != term);
  }

  private void pruneExpiredRequests(long now) {
    pendingRequests.values().removeIf(
        request -> now - request.lastRequestAtMs >= leaseExpiryMs);
  }

  private String firstPendingRequest() {
    Iterator<String> iterator = pendingRequests.keySet().iterator();
    return iterator.hasNext() ? iterator.next() : null;
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

  private boolean isExpired(Lease lease, long now) {
    long startTime = lease.claimed ? lease.claimedAtMs : lease.createdAtMs;
    return now - startTime >= leaseExpiryMs;
  }

  private synchronized boolean startProcessing(String datanodeId,
      long leaseId) {
    Lease lease = pendingLeases.get(datanodeId);
    if (lease == null || lease.leaseId != leaseId || !lease.claimed
        || lease.processing) {
      return false;
    }
    if (isExpired(lease, clock.getAsLong())) {
      pendingLeases.remove(datanodeId);
      incrementLeaseExpired();
      updateOutstandingLeaseMetric();
      return false;
    }
    lease.processing = true;
    return true;
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
    private long claimedAtMs;
    private boolean processing;

    private Lease(long leaseId, long term, long createdAtMs,
        long deferredRegistrationId) {
      this.leaseId = leaseId;
      this.term = term;
      this.createdAtMs = createdAtMs;
      this.deferredRegistrationId = deferredRegistrationId;
    }
  }

  private static final class LeaseRequest {
    private final long term;
    private long lastRequestAtMs;

    private LeaseRequest(long term, long lastRequestAtMs) {
      this.term = term;
      this.lastRequestAtMs = lastRequestAtMs;
    }
  }

  static final class LeaseClaim implements ContainerReportProcessingLifecycle {
    private final SCMFullContainerReportLeaseManager leaseManager;
    private final String datanodeId;
    private final long leaseId;
    private final boolean registrationReport;

    private LeaseClaim(SCMFullContainerReportLeaseManager leaseManager,
        String datanodeId, long leaseId, boolean registrationReport) {
      this.leaseManager = leaseManager;
      this.datanodeId = datanodeId;
      this.leaseId = leaseId;
      this.registrationReport = registrationReport;
    }

    boolean isRegistrationReport() {
      return registrationReport;
    }

    @Override
    public boolean startProcessing() {
      return leaseManager.startProcessing(datanodeId, leaseId);
    }

    @Override
    public void complete(boolean processed) {
      leaseManager.completeLease(datanodeId, leaseId, processed);
    }
  }
}
