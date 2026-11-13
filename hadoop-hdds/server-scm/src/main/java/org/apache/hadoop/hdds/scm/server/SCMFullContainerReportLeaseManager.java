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
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.LongSupplier;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;

/**
 * Reserves capacity for outstanding full container reports on one SCM.
 * Created and owned by {@link StorageContainerManager}, and shared with its datanode protocol server.
 *
 * <p>Every outstanding lease counts toward the capacity limit, from grant until report completion or discard:
 * <pre>
 * GRANTED -> CLAIMED -> PROCESSING -> removed
 *                \---- discarded -> removed
 * </pre>
 * A GRANTED lease expires if the datanode does not send its report in time. Claiming validates the grant and marks it
 * CLAIMED, retaining capacity while the report is dispatched, queued or processed. CLAIMED and PROCESSING leases do not
 * expire and survive datanode removal until the report completes or is discarded. Local leadership changes do not
 * invalidate outstanding leases.
 * Disabling rate limiting bypasses only the capacity check; validation and duplicate-report checks still apply.
 *
 * <p>A deferred registration stamp detects intervening registrations, similar to the version stamp in
 * {@link java.util.concurrent.atomic.AtomicStampedReference} or optimistic-read validation in
 * {@link java.util.concurrent.locks.StampedLock}. Each grant captures the current stamp, or zero if no registration is
 * pending. Successful completion clears only a matching stamp, so an older report cannot clear a newer registration's
 * pending FCR. Stamps do not provide synchronization; the manager's monitor does. Stamps and leases are local,
 * in-memory state, not replicated metadata.
 *
 * <p>This object's monitor guards all mutable manager and {@link Lease} state. Expired grants are removed during
 * lease requests, rather than by a background task. Callers determine upgrade and SCM eligibility before requesting or
 * claiming leases; this class does not consult SCM leadership or version state.
 */
public class SCMFullContainerReportLeaseManager {
  private final int maxOutstandingLeases;
  private final long leaseExpiryMs;
  private final LongSupplier clock;
  private final SCMFullContainerReportLeaseMetrics metrics;
  private final Map<String, Lease> outstandingLeases = new HashMap<>();
  // Equal durations and a monotonic clock make insertion order the expiry order.
  private final Map<String, Lease> grantedLeases = new LinkedHashMap<>();
  private final Map<String, Long> deferredRegistrationStamps = new HashMap<>();
  private final Set<String> removedDatanodes = new HashSet<>();
  private long nextDeferredRegistrationStamp;
  private boolean rateLimitEnabled;

  public SCMFullContainerReportLeaseManager(int maxOutstandingLeases, long leaseExpiryMs, boolean rateLimitEnabled,
      LongSupplier clock, SCMFullContainerReportLeaseMetrics metrics) {
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
    this.rateLimitEnabled = rateLimitEnabled;
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
   * @return the new grant, or empty if capacity is unavailable or this datanode already has a claimed lease
   */
  public synchronized OptionalLong requestLease(DatanodeDetails datanode) {
    incrementLeaseRequests();
    long now = clock.getAsLong();
    pruneExpiredGrants(now);
    String datanodeId = datanode.getUuidString();
    if (removedDatanodes.contains(datanodeId)) {
      incrementLeasesRejected();
      return OptionalLong.empty();
    }
    Lease existingLease = outstandingLeases.get(datanodeId);
    if (existingLease != null && existingLease.state != LeaseState.GRANTED) {
      incrementLeasesRejected();
      updateOutstandingLeaseMetric();
      return OptionalLong.empty();
    }
    int outstandingAfterReplacement = outstandingLeases.size() - (existingLease == null ? 0 : 1);
    if (rateLimitEnabled && outstandingAfterReplacement >= maxOutstandingLeases) {
      incrementLeasesRejected();
      updateOutstandingLeaseMetric();
      return OptionalLong.empty();
    }

    long leaseId = nextLeaseId();
    long deferredRegistrationStamp = deferredRegistrationStamps.getOrDefault(datanodeId, 0L);
    Lease lease = new Lease(leaseId, now, deferredRegistrationStamp);
    outstandingLeases.put(datanodeId, lease);
    grantedLeases.remove(datanodeId);
    grantedLeases.put(datanodeId, lease);
    incrementLeasesGranted();
    updateOutstandingLeaseMetric();
    return OptionalLong.of(leaseId);
  }

  /**
   * Validates an incoming report's grant and moves it from GRANTED to CLAIMED without releasing capacity.
   *
   * @param datanode datanode sending the FCR
   * @param leaseId lease ID attached to the incoming report
   * @return the report's lease claim, or empty if the grant is invalid, expired or already claimed
   */
  public synchronized Optional<LeaseClaim> claimLease(DatanodeDetails datanode, long leaseId) {
    if (leaseId == 0) {
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
      grantedLeases.remove(datanodeId);
      incrementLeasesExpired();
      incrementInvalidLeaseReports();
      updateOutstandingLeaseMetric();
      return Optional.empty();
    }

    if (lease.leaseId != leaseId) {
      incrementInvalidLeaseReports();
      return Optional.empty();
    }

    lease.state = LeaseState.CLAIMED;
    grantedLeases.remove(datanodeId);
    return Optional.of(new LeaseClaim(this, datanodeId, leaseId));
  }

  private synchronized boolean isRegistrationReport(String datanodeId, long leaseId) {
    Lease lease = outstandingLeases.get(datanodeId);
    return lease != null && lease.leaseId == leaseId && lease.deferredRegistrationStamp != 0
        && deferredRegistrationStamps.getOrDefault(datanodeId, 0L) == lease.deferredRegistrationStamp;
  }

  private synchronized boolean startProcessing(String datanodeId, long leaseId) {
    Lease lease = outstandingLeases.get(datanodeId);
    if (lease == null || lease.leaseId != leaseId || lease.state != LeaseState.CLAIMED) {
      return false;
    }
    lease.state = LeaseState.PROCESSING;
    return true;
  }

  private synchronized boolean completeLease(String datanodeId, long leaseId,
      boolean reportProcessed) {
    Lease lease = outstandingLeases.get(datanodeId);
    if (lease == null || lease.leaseId != leaseId) {
      return false;
    }
    boolean registrationReport = reportProcessed && isRegistrationReport(datanodeId, leaseId);
    outstandingLeases.remove(datanodeId);
    grantedLeases.remove(datanodeId);
    if (reportProcessed) {
      if (registrationReport) {
        deferredRegistrationStamps.remove(datanodeId);
      }
      incrementReportsProcessedWithLease();
    }
    updateOutstandingLeaseMetric();
    return registrationReport;
  }

  /**
   * Records a new registration that still needs an FCR and invalidates any earlier unclaimed grant.
   * A claimed lease retains capacity, but its completion cannot clear this newer registration's stamp.
   */
  public synchronized void markFullContainerReportDeferred(
      DatanodeDetails datanode) {
    long deferredRegistrationStamp = ++nextDeferredRegistrationStamp;
    if (deferredRegistrationStamp == 0) {
      deferredRegistrationStamp = ++nextDeferredRegistrationStamp;
    }
    String datanodeId = datanode.getUuidString();
    removedDatanodes.remove(datanodeId);
    deferredRegistrationStamps.put(datanodeId, deferredRegistrationStamp);
    Lease lease = outstandingLeases.get(datanodeId);
    if (lease != null && lease.state == LeaseState.GRANTED) {
      outstandingLeases.remove(datanodeId);
      grantedLeases.remove(datanodeId);
      updateOutstandingLeaseMetric();
    }
  }

  /**
   * Clears the pending registration stamp when registration supplies an actual, unleased FCR.
   */
  public synchronized void clearFullContainerReportDeferred(
      DatanodeDetails datanode) {
    String datanodeId = datanode.getUuidString();
    removedDatanodes.remove(datanodeId);
    deferredRegistrationStamps.remove(datanodeId);
  }

  public synchronized boolean isFullContainerReportDeferred(DatanodeDetails datanode) {
    return deferredRegistrationStamps.containsKey(datanode.getUuidString());
  }

  /**
   * Removes registration tracking and any unclaimed grant, and rejects new grants until successful registration.
   * Claimed leases must be completed by their reports.
   */
  public synchronized void removeDatanode(DatanodeDetails datanode) {
    String datanodeId = datanode.getUuidString();
    removedDatanodes.add(datanodeId);
    Lease lease = outstandingLeases.get(datanodeId);
    if (lease != null && lease.state == LeaseState.GRANTED) {
      outstandingLeases.remove(datanodeId);
      grantedLeases.remove(datanodeId);
      updateOutstandingLeaseMetric();
    }
    deferredRegistrationStamps.remove(datanodeId);
  }

  private long nextLeaseId() {
    long leaseId = 0;
    while (leaseId == 0) {
      leaseId = ThreadLocalRandom.current().nextLong();
    }
    return leaseId;
  }

  private void pruneExpiredGrants(long now) {
    Iterator<Map.Entry<String, Lease>> iterator = grantedLeases.entrySet().iterator();
    boolean removed = false;
    while (iterator.hasNext()) {
      Map.Entry<String, Lease> entry = iterator.next();
      if (!isExpired(entry.getValue(), now)) {
        break;
      }
      iterator.remove();
      outstandingLeases.remove(entry.getKey(), entry.getValue());
      incrementLeasesExpired();
      removed = true;
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

  private void incrementLeasesExpired() {
    if (metrics != null) {
      metrics.incNumFCRLeasesExpired();
    }
  }

  /**
   * Counts reports with missing or invalid leases, including rejections by the protocol server's eligibility checks.
   */
  void incrementInvalidLeaseReports() {
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
    private final long createdAtMs;
    private final long deferredRegistrationStamp;
    private LeaseState state = LeaseState.GRANTED;

    private Lease(long leaseId, long createdAtMs, long deferredRegistrationStamp) {
      this.leaseId = leaseId;
      this.createdAtMs = createdAtMs;
      this.deferredRegistrationStamp = deferredRegistrationStamp;
    }
  }

  /**
   * Holds a claimed FCR lease until the report completes.
   * The claim remains valid while its report is queued, even after grant expiry or SCM leadership changes.
   */
  public static final class LeaseClaim {
    private final SCMFullContainerReportLeaseManager leaseManager;
    private final String datanodeId;
    private final long leaseId;

    private LeaseClaim(SCMFullContainerReportLeaseManager leaseManager,
        String datanodeId, long leaseId) {
      this.leaseManager = leaseManager;
      this.datanodeId = datanodeId;
      this.leaseId = leaseId;
    }

    /**
     * Whether this claim still matches the current deferred registration, before completion.
     */
    boolean isRegistrationReport() {
      return leaseManager.isRegistrationReport(datanodeId, leaseId);
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
     * and clears its deferred registration stamp only if that registration is still current.
     * Repeated completion cannot release a newer lease for the same datanode.
     *
     * @param processed true if report processing completed successfully; false on failure or discard
     * @return true only when successful completion consumed the current deferred registration
     */
    public boolean complete(boolean processed) {
      return leaseManager.completeLease(datanodeId, leaseId, processed);
    }
  }
}
