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
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.LongSupplier;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;

/**
 * Reserves capacity for outstanding full container reports on one SCM.
 *
 * <p>Every outstanding lease counts toward the capacity limit, from grant until report completion or discard:
 * <pre>
 * GRANTED -> CLAIMED -> PROCESSING -> removed
 *                \---- discarded -> removed
 * </pre>
 * A GRANTED lease expires if the datanode does not send its report in time. Claiming validates the grant and marks it
 * CLAIMED, retaining capacity while the report is dispatched, queued or processed. CLAIMED and PROCESSING leases do not
 * expire and survive local leadership changes and datanode removal until the report completes or is discarded.
 * Disabling rate limiting bypasses only the capacity check; validation and duplicate-report checks still apply.
 *
 * <p>Deferred registration IDs identify registrations whose FCR has not completed. Each grant captures the current
 * ID, or zero if none is pending. Successful completion clears only that same ID, so an older report cannot clear
 * the pending FCR of a newer registration. These IDs and leases are local, in-memory state, not replicated metadata.
 *
 * <p>This object's monitor guards all mutable manager and {@link Lease} state. Expired grants and grants from other
 * terms are removed during lease requests, rather than by a background task. Callers determine upgrade and SCM
 * eligibility before requesting or claiming leases; this class does not consult SCM leadership or version state.
 */
public class SCMFullContainerReportLeaseManager {
  private final int maxOutstandingLeases;
  private final long leaseExpiryMs;
  private final LongSupplier clock;
  private final SCMFullContainerReportLeaseMetrics metrics;
  private final Map<String, Lease> outstandingLeases = new HashMap<>();
  private final Map<String, Long> deferredRegistrationIds = new HashMap<>();
  private long nextDeferredRegistrationId;
  private boolean rateLimitEnabled = true;

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
    if (metrics != null) {
      metrics.setFCRRateLimitEnabled(rateLimitEnabled);
    }
  }

  /**
   * Enables or bypasses capacity checks for new grants without changing outstanding leases or report validation.
   */
  public synchronized void setRateLimitEnabled(boolean enabled) {
    rateLimitEnabled = enabled;
    if (metrics != null) {
      metrics.setFCRRateLimitEnabled(enabled);
    }
  }

  /**
   * Grants a nonzero lease ID if capacity is available, replacing any unclaimed grant for this datanode.
   *
   * @param datanode datanode requesting permission to send an FCR
   * @param term term selected by the caller's SCM eligibility policy
   * @return the new grant, or empty if capacity is unavailable or this datanode already has a claimed lease
   */
  public synchronized OptionalLong requestLease(
      DatanodeDetails datanode, long term) {
    incrementLeaseRequests();
    long now = clock.getAsLong();
    pruneExpiredGrants(now);
    removeGrantsFromOtherTerms(term);
    String datanodeId = datanode.getUuidString();
    Lease existingLease = outstandingLeases.get(datanodeId);
    if (existingLease != null && existingLease.state != LeaseState.GRANTED) {
      incrementLeasesRejected();
      updateOutstandingLeaseMetric();
      return OptionalLong.empty();
    }
    outstandingLeases.remove(datanodeId);

    if (rateLimitEnabled && outstandingLeases.size() >= maxOutstandingLeases) {
      incrementLeasesRejected();
      updateOutstandingLeaseMetric();
      return OptionalLong.empty();
    }

    long leaseId = nextLeaseId();
    long deferredRegistrationId =
        deferredRegistrationIds.getOrDefault(datanodeId, 0L);
    outstandingLeases.put(datanodeId,
        new Lease(leaseId, term, now, deferredRegistrationId));
    incrementLeasesGranted();
    updateOutstandingLeaseMetric();
    return OptionalLong.of(leaseId);
  }

  /**
   * Validates an incoming report's grant and moves it from GRANTED to CLAIMED without releasing capacity.
   *
   * @param datanode datanode sending the FCR
   * @param currentTerm currently allowed lease term, or empty when leasing is unavailable
   * @param leaseTerm term attached to the incoming report
   * @param leaseId lease ID attached to the incoming report
   * @return the report's lease claim, or empty if the grant is invalid, expired or already claimed
   */
  public synchronized Optional<LeaseClaim> claimLease(
      DatanodeDetails datanode, OptionalLong currentTerm, long leaseTerm,
      long leaseId) {
    if (!currentTerm.isPresent() || currentTerm.getAsLong() != leaseTerm
        || leaseId == 0) {
      incrementInvalidLeaseReports();
      return Optional.empty();
    }

    String datanodeId = datanode.getUuidString();
    Lease lease = outstandingLeases.get(datanodeId);
    if (lease == null) {
      incrementInvalidLeaseReports();
      return Optional.empty();
    }

    if (lease.state != LeaseState.GRANTED) {
      incrementInvalidLeaseReports();
      return Optional.empty();
    }

    if (isExpired(lease, clock.getAsLong())) {
      outstandingLeases.remove(datanodeId);
      incrementLeaseExpired();
      incrementInvalidLeaseReports();
      updateOutstandingLeaseMetric();
      return Optional.empty();
    }

    if (lease.term != currentTerm.getAsLong()) {
      if (currentTerm.getAsLong() > lease.term) {
        outstandingLeases.remove(datanodeId);
        updateOutstandingLeaseMetric();
      }
      incrementInvalidLeaseReports();
      return Optional.empty();
    }

    if (lease.leaseId != leaseId) {
      incrementInvalidLeaseReports();
      return Optional.empty();
    }

    lease.state = LeaseState.CLAIMED;
    return Optional.of(new LeaseClaim(this, datanodeId, leaseId,
        lease.deferredRegistrationId != 0));
  }

  private synchronized boolean startProcessing(String datanodeId, long leaseId) {
    Lease lease = outstandingLeases.get(datanodeId);
    if (lease == null || lease.leaseId != leaseId || lease.state != LeaseState.CLAIMED) {
      return false;
    }
    lease.state = LeaseState.PROCESSING;
    return true;
  }

  private synchronized void completeLease(String datanodeId, long leaseId,
      boolean reportProcessed) {
    Lease lease = outstandingLeases.get(datanodeId);
    if (lease == null || lease.leaseId != leaseId) {
      return;
    }
    outstandingLeases.remove(datanodeId);
    if (reportProcessed) {
      if (lease.deferredRegistrationId != 0
          && deferredRegistrationIds.getOrDefault(datanodeId, 0L)
          == lease.deferredRegistrationId) {
        deferredRegistrationIds.remove(datanodeId);
      }
      incrementReportsProcessedWithLease();
    }
    updateOutstandingLeaseMetric();
  }

  /**
   * Records a new registration that still needs an FCR and invalidates any earlier unclaimed grant.
   * A claimed lease retains capacity, but its completion cannot clear this newer registration's ID.
   */
  public synchronized void markFullContainerReportDeferred(
      DatanodeDetails datanode) {
    long deferredRegistrationId = ++nextDeferredRegistrationId;
    if (deferredRegistrationId == 0) {
      deferredRegistrationId = ++nextDeferredRegistrationId;
    }
    String datanodeId = datanode.getUuidString();
    deferredRegistrationIds.put(datanodeId, deferredRegistrationId);
    Lease lease = outstandingLeases.get(datanodeId);
    if (lease != null && lease.state == LeaseState.GRANTED) {
      outstandingLeases.remove(datanodeId);
      updateOutstandingLeaseMetric();
    }
  }

  /**
   * Clears the pending registration ID when registration supplies an actual, unleased FCR.
   */
  public synchronized void clearFullContainerReportDeferred(
      DatanodeDetails datanode) {
    deferredRegistrationIds.remove(datanode.getUuidString());
  }

  /**
   * Removes registration tracking and any unclaimed grant. Claimed leases must be completed by their reports.
   */
  public synchronized void removeDatanode(DatanodeDetails datanode) {
    String datanodeId = datanode.getUuidString();
    Lease lease = outstandingLeases.get(datanodeId);
    if (lease != null && lease.state == LeaseState.GRANTED) {
      outstandingLeases.remove(datanodeId);
      updateOutstandingLeaseMetric();
    }
    deferredRegistrationIds.remove(datanodeId);
  }

  private long nextLeaseId() {
    long leaseId = 0;
    while (leaseId == 0) {
      leaseId = ThreadLocalRandom.current().nextLong();
    }
    return leaseId;
  }

  private void pruneExpiredGrants(long now) {
    Iterator<Map.Entry<String, Lease>> iterator = outstandingLeases.entrySet()
        .iterator();
    boolean removed = false;
    while (iterator.hasNext()) {
      Lease lease = iterator.next().getValue();
      if (lease.state == LeaseState.GRANTED && isExpired(lease, now)) {
        iterator.remove();
        incrementLeaseExpired();
        removed = true;
      }
    }
    if (removed) {
      updateOutstandingLeaseMetric();
    }
  }

  private void removeGrantsFromOtherTerms(long term) {
    Iterator<Map.Entry<String, Lease>> iterator = outstandingLeases.entrySet()
        .iterator();
    boolean removed = false;
    while (iterator.hasNext()) {
      Lease lease = iterator.next().getValue();
      if (lease.state == LeaseState.GRANTED && lease.term != term) {
        iterator.remove();
        removed = true;
      }
    }
    if (removed) {
      updateOutstandingLeaseMetric();
    }
  }

  private boolean isExpired(Lease lease, long now) {
    return now - lease.createdAtMs >= leaseExpiryMs;
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
      metrics.setNumFCRLeasesOutstanding(outstandingLeases.size());
    }
  }

  private enum LeaseState {
    GRANTED,
    CLAIMED,
    PROCESSING
  }

  private static final class Lease {
    private final long leaseId;
    private final long term;
    private final long createdAtMs;
    private final long deferredRegistrationId;
    private LeaseState state = LeaseState.GRANTED;

    private Lease(long leaseId, long term, long createdAtMs,
        long deferredRegistrationId) {
      this.leaseId = leaseId;
      this.term = term;
      this.createdAtMs = createdAtMs;
      this.deferredRegistrationId = deferredRegistrationId;
    }
  }

  /**
   * Holds a claimed FCR lease until the report completes.
   * The claim remains valid while its report is queued, even after grant expiry or term changes.
   */
  public static final class LeaseClaim {
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

    /**
     * Whether this claim's grant captured a deferred registration whose FCR must advance container SafeMode rules.
     */
    boolean isRegistrationReport() {
      return registrationReport;
    }

    /**
     * Moves the lease from CLAIMED to PROCESSING once, without rechecking grant expiry or SCM eligibility.
     *
     * @return false if the claim was already completed or processing has already started
     */
    public boolean startProcessing() {
      return leaseManager.startProcessing(datanodeId, leaseId);
    }

    /**
     * Releases the matching lease's capacity in either outcome. A successful report also updates success metrics
     * and clears its deferred registration ID only if that registration is still current.
     * Repeated completion cannot release a newer lease for the same datanode.
     *
     * @param processed true if report processing completed successfully; false on failure or discard
     */
    public void complete(boolean processed) {
      leaseManager.completeLease(datanodeId, leaseId, processed);
    }
  }
}
