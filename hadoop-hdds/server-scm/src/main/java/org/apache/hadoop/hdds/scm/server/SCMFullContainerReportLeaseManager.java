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
    pendingLeases.remove(datanode.getUuidString());

    if (pendingLeases.size() >= maxOutstandingLeases) {
      incrementLeasesRejected();
      updateOutstandingLeaseMetric();
      return 0;
    }

    long leaseId = nextLeaseId();
    pendingLeases.put(datanode.getUuidString(),
        new Lease(leaseId, term, clock.getAsLong()));
    incrementLeasesGranted();
    updateOutstandingLeaseMetric();
    return leaseId;
  }

  public synchronized boolean checkLease(DatanodeDetails datanode, long term,
      long leaseId) {
    if (leaseId == 0) {
      incrementInvalidLeaseReports();
      return false;
    }

    Lease lease = pendingLeases.get(datanode.getUuidString());
    if (lease == null) {
      incrementInvalidLeaseReports();
      return false;
    }

    if (isExpired(lease)) {
      pendingLeases.remove(datanode.getUuidString());
      incrementLeaseExpired();
      incrementInvalidLeaseReports();
      updateOutstandingLeaseMetric();
      return false;
    }

    if (lease.term != term) {
      if (term > lease.term) {
        pendingLeases.remove(datanode.getUuidString());
        updateOutstandingLeaseMetric();
      }
      incrementInvalidLeaseReports();
      return false;
    }

    if (lease.leaseId != leaseId) {
      incrementInvalidLeaseReports();
      return false;
    }

    return true;
  }

  public synchronized void removeLease(DatanodeDetails datanode) {
    pendingLeases.remove(datanode.getUuidString());
    updateOutstandingLeaseMetric();
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
      if (isExpired(iterator.next().getValue())) {
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
      if (iterator.next().getValue().term != term) {
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

  private void updateOutstandingLeaseMetric() {
    if (metrics != null) {
      metrics.setNumFCRLeasesOutstanding(pendingLeases.size());
    }
  }

  private static final class Lease {
    private final long leaseId;
    private final long term;
    private final long createdAtMs;

    private Lease(long leaseId, long term, long createdAtMs) {
      this.leaseId = leaseId;
      this.term = term;
      this.createdAtMs = createdAtMs;
    }
  }
}
