/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.service;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.utils.db.CodecRegistry;
import org.apache.hadoop.hdds.utils.db.IntegerCodec;
import org.apache.hadoop.hdds.utils.db.managed.ManagedColumnFamilyOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedDBOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksIterator;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.codec.OmDBDiffReportEntryCodec;
import org.apache.hadoop.ozone.om.snapshot.SnapshotDiffJob;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffReportOzone;
import org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.JobStatus;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

import static org.apache.hadoop.ozone.om.OmSnapshotManager.DELIMITER;
import static org.apache.hadoop.hdds.utils.db.DBStoreBuilder.DEFAULT_COLUMN_FAMILY_NAME;
import static org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.JobStatus.DONE;
import static org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.JobStatus.FAILED;
import static org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.JobStatus.IN_PROGRESS;
import static org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.JobStatus.QUEUED;
import static org.apache.hadoop.ozone.snapshot.SnapshotDiffResponse.JobStatus.REJECTED;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

/**
 * Tests SnapshotDiffCleanupService.
 */
public class TestSnapshotDiffCleanupService {
  private static File file;
  private static ManagedRocksDB db;
  private static ManagedDBOptions dbOptions;
  private static ManagedColumnFamilyOptions columnFamilyOptions;
  private final byte[] jobTableNameBytes =
      StringUtils.string2Bytes("snap-diff-job-table");
  private final byte[] purgedJobTableNameBytes =
      StringUtils.string2Bytes("snap-diff-purged-job-table");
  private final byte[] reportTableNameBytes =
      StringUtils.string2Bytes("snap-diff-report-table");
  private ColumnFamilyDescriptor jobTableCfd;
  private ColumnFamilyDescriptor purgedJobTableCfd;
  private ColumnFamilyDescriptor reportTableCfd;
  private ColumnFamilyHandle jobTableCfh;
  private ColumnFamilyHandle purgedJobTableCfh;
  private ColumnFamilyHandle reportTableCfh;
  private CodecRegistry codecRegistry;
  private byte[] emptyReportEntry;
  private SnapshotDiffCleanupService diffCleanupService;
  @Mock
  private OzoneManager ozoneManager;

  @BeforeAll
  public static void staticInit() throws RocksDBException {
    dbOptions = new ManagedDBOptions();
    dbOptions.setCreateIfMissing(true);
    columnFamilyOptions = new ManagedColumnFamilyOptions();

    file = new File("./test-snap-diff-clean-up");
    if (!file.mkdirs() && !file.exists()) {
      throw new IllegalArgumentException("Unable to create directory " +
          file);
    }

    String absolutePath = Paths.get(file.toString(), "snapDiff.db").toFile()
        .getAbsolutePath();

    List<ColumnFamilyDescriptor> columnFamilyDescriptors =
        Collections.singletonList(new ColumnFamilyDescriptor(
            StringUtils.string2Bytes(DEFAULT_COLUMN_FAMILY_NAME),
            columnFamilyOptions));

    List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

    db = ManagedRocksDB.open(dbOptions, absolutePath, columnFamilyDescriptors,
        columnFamilyHandles);
  }

  @AfterAll
  public static void staticTearDown() {
    if (dbOptions != null) {
      dbOptions.close();
    }
    if (columnFamilyOptions != null) {
      columnFamilyOptions.close();
    }
    if (db != null) {
      db.close();
    }

    GenericTestUtils.deleteDirectory(file);
  }

  @BeforeEach
  public void init() throws RocksDBException, IOException {
    MockitoAnnotations.initMocks(this);
    when(ozoneManager.isLeaderReady()).thenReturn(true);

    jobTableCfd = new ColumnFamilyDescriptor(jobTableNameBytes,
        columnFamilyOptions);
    reportTableCfd = new ColumnFamilyDescriptor(reportTableNameBytes,
        columnFamilyOptions);
    purgedJobTableCfd = new ColumnFamilyDescriptor(purgedJobTableNameBytes,
        columnFamilyOptions);
    jobTableCfh = db.get().createColumnFamily(jobTableCfd);
    purgedJobTableCfh = db.get().createColumnFamily(purgedJobTableCfd);
    reportTableCfh = db.get().createColumnFamily(reportTableCfd);

    codecRegistry = new CodecRegistry();

    // Integers are used for indexing persistent list.
    codecRegistry.addCodec(Integer.class, new IntegerCodec());
    // DiffReportEntry codec for Diff Report.
    codecRegistry.addCodec(SnapshotDiffReportOzone.DiffReportEntry.class,
        new OmDBDiffReportEntryCodec());
    codecRegistry.addCodec(SnapshotDiffJob.class,
        new SnapshotDiffJob.SnapshotDiffJobCodec());
    emptyReportEntry = codecRegistry.asRawData("{}");

    diffCleanupService = new SnapshotDiffCleanupService(
        Duration.ofHours(1).toMillis(),
        Duration.ofSeconds(3).toMillis(),
        ozoneManager,
        db,
        jobTableCfh,
        purgedJobTableCfh,
        reportTableCfh,
        codecRegistry
    );
  }

  @AfterEach
  public void tearDown() {
    if (diffCleanupService != null) {
      diffCleanupService.shutdown();
    }
    if (jobTableCfh != null) {
      jobTableCfh.close();
    }
    if (purgedJobTableCfh != null) {
      purgedJobTableCfh.close();
    }
    if (reportTableCfh != null) {
      reportTableCfh.close();
    }
    if (jobTableCfd != null) {
      ManagedColumnFamilyOptions.closeDeeply(jobTableCfd.getOptions());
    }
    if (purgedJobTableCfd != null) {
      ManagedColumnFamilyOptions.closeDeeply(purgedJobTableCfd.getOptions());
    }
    if (reportTableCfd != null) {
      ManagedColumnFamilyOptions.closeDeeply(reportTableCfd.getOptions());
    }
  }

  @Test
  public void testSnapshotDiffCleanUpService()
      throws RocksDBException, IOException {
    // Suspend before adding jobs and reports to tables to get the consistent
    // behaviour.
    diffCleanupService.suspend();

    long currentTime = System.currentTimeMillis() - 1;

    // Add a valid DONE snapDiff job and report to DB.
    SnapshotDiffJob validRequest = addJobAndReport(DONE,
        currentTime - Duration.ofDays(1).toMillis(), 2);
    // Add a stale DONE snapDiff job and report to DB.
    SnapshotDiffJob staleRequest = addJobAndReport(DONE,
        currentTime - Duration.ofDays(10).toMillis(), 2);
    // Add a QUEUED snapDiff job to DB.
    SnapshotDiffJob queueJob = addJobAndReport(QUEUED,
        currentTime, 0);
    // Add an IN_PROGRESS snapDiff job and report to DB.
    SnapshotDiffJob inProgressJob1 = addJobAndReport(IN_PROGRESS,
        currentTime - Duration.ofMinutes(2).toMillis(), 12);
    // Add an IN_PROGRESS snapDiff job and report to DB.
    SnapshotDiffJob inProgressJob2 = addJobAndReport(IN_PROGRESS,
        currentTime - Duration.ofMinutes(1).toMillis(), 5);
    // Add a FAILED snapDiff job and report to DB.
    SnapshotDiffJob failedJob = addJobAndReport(FAILED,
        currentTime, 10);
    // Add a REJECTED snapDiff job and report to DB.
    SnapshotDiffJob recentRejectedJob = addJobAndReport(REJECTED,
        currentTime, 0);
    // Add a stale and REJECTED snapDiff job and report to DB.
    SnapshotDiffJob staleRejectedJob = addJobAndReport(REJECTED,
        currentTime - Duration.ofDays(10).toMillis(), 0);

    diffCleanupService.resume();

    // Run 1.
    diffCleanupService.run();
    // Assert nothing is remove from report table and only entries were moved
    // from active job table to purge job table.
    assertJobInActiveTable(getJobKey(validRequest), validRequest);
    assertJobInActiveTable(getJobKey(queueJob), queueJob);
    assertJobInActiveTable(getJobKey(inProgressJob1), inProgressJob1);
    assertJobInActiveTable(getJobKey(inProgressJob2), inProgressJob2);
    assertJobInPurgedTable(staleRequest.getJobId(),
        staleRequest.getTotalDiffEntries());
    assertJobInPurgedTable(failedJob.getJobId(),
        failedJob.getTotalDiffEntries());
    assertJobInPurgedTable(staleRejectedJob.getJobId(),
        staleRejectedJob.getTotalDiffEntries());
    assertJobInPurgedTable(recentRejectedJob.getJobId(),
        recentRejectedJob.getTotalDiffEntries());
    assertNumberOfEntriesInTable(jobTableCfh, 4);
    assertNumberOfEntriesInTable(purgedJobTableCfh, 4);
    assertNumberOfEntriesInTable(reportTableCfh, 31);

    // Run 2.
    diffCleanupService.run();
    // Asset report table was cleaned.
    assertJobAndReport(validRequest, true);
    assertJobAndReport(staleRequest, false);
    assertJobAndReport(queueJob, true);
    assertJobAndReport(inProgressJob1, true);
    assertJobAndReport(inProgressJob2, true);
    assertJobAndReport(failedJob, false);
    assertJobAndReport(recentRejectedJob, false);
    assertJobAndReport(staleRejectedJob, false);

    assertNumberOfEntriesInTable(jobTableCfh, 4);
    assertNumberOfEntriesInTable(purgedJobTableCfh, 0);
    assertNumberOfEntriesInTable(reportTableCfh, 19);
  }

  private SnapshotDiffJob addJobAndReport(JobStatus jobStatus,
                                          long creationTime,
                                          long noOfEntries)
      throws IOException, RocksDBException {

    String jobId = "jobId-" + RandomStringUtils.randomAlphanumeric(10);
    String volume = "volume-" + RandomStringUtils.randomAlphanumeric(10);
    String bucket = "bucket-" + RandomStringUtils.randomAlphanumeric(10);
    String fromSnapshot = "fromSnap-" +
        RandomStringUtils.randomAlphanumeric(10);
    String toSnapshot = "toSnap-" + RandomStringUtils.randomAlphanumeric(10);
    String jobKey = fromSnapshot + DELIMITER + toSnapshot;

    SnapshotDiffJob job = new SnapshotDiffJob(creationTime, jobId, jobStatus,
        volume, bucket, fromSnapshot, toSnapshot, false, noOfEntries);

    db.get().put(jobTableCfh, codecRegistry.asRawData(jobKey),
        codecRegistry.asRawData(job));

    if (jobStatus == REJECTED || jobStatus == QUEUED) {
      return job;
    }

    for (int i = 0; i < noOfEntries; i++) {
      db.get().put(reportTableCfh,
          codecRegistry.asRawData(jobId + DELIMITER + i),
          emptyReportEntry);
    }
    return job;
  }

  private void assertJobAndReport(SnapshotDiffJob expectedJob,
                                  boolean isExpected)
      throws IOException, RocksDBException {
    String jobKey =
        expectedJob.getFromSnapshot() + DELIMITER + expectedJob.getToSnapshot();
    if (isExpected) {
      assertJobInActiveTable(jobKey, expectedJob);
      assertReport(expectedJob.getJobId(),
          expectedJob.getTotalDiffEntries(),
          emptyReportEntry);
    } else {
      assertJobInActiveTable(jobKey, null);
      assertReport(expectedJob.getJobId(),
          expectedJob.getTotalDiffEntries(),
          null);
    }
  }

  private void assertJobInActiveTable(String jobKey,
                                      SnapshotDiffJob expectedJob)
      throws IOException, RocksDBException {
    byte[] bytes = db.get().get(jobTableCfh, codecRegistry.asRawData(jobKey));
    SnapshotDiffJob actualJob =
        codecRegistry.asObject(bytes, SnapshotDiffJob.class);

    assertEquals(expectedJob, actualJob);
  }

  private void assertReport(String jobId,
                            long noOfEntries,
                            byte[] expectedEntry)
      throws IOException, RocksDBException {

    for (int index = 0; index < noOfEntries; index++) {
      byte[] bytes = db.get().get(reportTableCfh,
          codecRegistry.asRawData(jobId + DELIMITER + index));
      assertArrayEquals(expectedEntry, bytes);
    }
  }

  private void assertJobInPurgedTable(String jobKey,
                                      long expectedEntriesCount)
      throws IOException, RocksDBException {
    byte[] bytes = db.get().get(purgedJobTableCfh,
        codecRegistry.asRawData(jobKey));
    long actualEntriesCount = codecRegistry.asObject(bytes, Long.class);
    assertEquals(expectedEntriesCount, actualEntriesCount);
  }


  private void assertNumberOfEntriesInTable(ColumnFamilyHandle table,
                                            long expectedCount) {
    int count = 0;
    try (ManagedRocksIterator iterator =
             new ManagedRocksIterator(db.get().newIterator(table))) {
      iterator.get().seekToFirst();
      while (iterator.get().isValid()) {
        iterator.get().next();
        count++;
      }
    }

    assertEquals(expectedCount, count);
  }

  private String getJobKey(SnapshotDiffJob diffJob) {
    return diffJob.getFromSnapshot() + DELIMITER + diffJob.getToSnapshot();
  }
}
