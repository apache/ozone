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

package org.apache.hadoop.ozone.recon.api;

import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_EXPORT_DIRECTORY;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_EXPORT_MAX_DOWNLOADS;
import static org.apache.hadoop.ozone.recon.ReconServerConfigKeys.OZONE_RECON_EXPORT_MAX_JOBS_TOTAL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.recon.api.types.ExportJob;
import org.apache.hadoop.ozone.recon.api.types.ExportJob.JobStatus;
import org.apache.hadoop.ozone.recon.persistence.ContainerHealthSchemaManager;
import org.apache.ozone.recon.schema.ContainerSchemaDefinition.UnHealthyContainerStates;
import org.apache.ozone.recon.schema.generated.tables.records.UnhealthyContainersRecord;
import org.jooq.Cursor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit tests for {@link ExportJobManager}.
 *
 * <p>The manager only depends on {@link ContainerHealthSchemaManager} for DB
 * access; that collaborator is mocked so the tests run end-to-end through the
 * single-thread worker without spinning up Derby or Guice.</p>
 */
class TestExportJobManager {

  private static final String STATE_MISSING = "MISSING";
  private static final String STATE_UNDER_REPLICATED = "UNDER_REPLICATED";
  private static final String STATE_OVER_REPLICATED = "OVER_REPLICATED";
  private static final Duration DEFAULT_AWAIT = Duration.ofSeconds(15);

  @TempDir
  private Path tempDir;

  private ExportJobManager manager;
  private ContainerHealthSchemaManager schema;

  @BeforeEach
  void setUp() {
    schema = mock(ContainerHealthSchemaManager.class);
    manager = newManager(/* maxQueueSize */ 4);
  }

  @AfterEach
  void tearDown() {
    if (manager != null) {
      manager.shutdown();
    }
  }

  // ── Submit path ──────────────────────────────────────────────────────────

  @Test
  void submitCompletesAndProducesTar() throws Exception {
    stubExport(STATE_MISSING, 5);

    String jobId = manager.submitJob(STATE_MISSING);
    awaitStatus(jobId, JobStatus.COMPLETED, DEFAULT_AWAIT);

    ExportJob job = manager.getJob(jobId);
    assertThat(job.getTotalRecords()).isEqualTo(5L);
    assertThat(job.getEstimatedTotal()).isEqualTo(5L);
    assertThat(new File(job.getFilePath())).exists().isFile();
    assertThat(job.getFilePath()).endsWith(".tar");
  }

  @Test
  void submitLargeExportSplitsIntoMultipleCsvParts() throws Exception {
    // Crosses one million records, which should produce 3 CSV chunks:
    // part001 (500k), part002 (500k), part003 (remaining).
    final int recordCount = 1_000_001;
    stubExport(STATE_MISSING, recordCount);

    String jobId = manager.submitJob(STATE_MISSING);
    awaitStatus(jobId, JobStatus.COMPLETED, Duration.ofMinutes(2));

    ExportJob job = manager.getJob(jobId);
    assertThat(job.getTotalRecords()).isEqualTo(recordCount);
    assertThat(new File(job.getFilePath())).exists().isFile();

    List<String> tarEntries = listTarEntryNames(new File(job.getFilePath()));
    assertThat(tarEntries).hasSize(3);
    assertThat(tarEntries).contains(
        "unhealthy_containers_missing_part001.csv",
        "unhealthy_containers_missing_part002.csv",
        "unhealthy_containers_missing_part003.csv");
  }

  @Test
  void submitEmptyResultStillReachesCompleted() throws Exception {
    stubExport(STATE_MISSING, 0);

    String jobId = manager.submitJob(STATE_MISSING);
    awaitStatus(jobId, JobStatus.COMPLETED, DEFAULT_AWAIT);

    ExportJob job = manager.getJob(jobId);
    assertThat(job.getTotalRecords()).isZero();
    assertThat(new File(job.getFilePath())).exists();
  }

  @Test
  void submitDuplicateStateWhileRunningIsRejected() throws Exception {
    CountDownLatch release = new CountDownLatch(1);
    when(schema.getUnhealthyContainersCount(any(), anyInt(), anyLong())).thenReturn(1L);
    when(schema.getUnhealthyContainersCursor(any(), anyInt(), anyLong()))
        .thenAnswer(inv -> blockingCursor(release));

    String firstJobId = manager.submitJob(STATE_MISSING);
    try {
      awaitStatus(firstJobId, JobStatus.RUNNING, DEFAULT_AWAIT);

      assertThatThrownBy(() -> manager.submitJob(STATE_MISSING))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("already exists");
    } finally {
      release.countDown();
      awaitStatus(firstJobId, JobStatus.COMPLETED, DEFAULT_AWAIT);
    }
  }

  @Test
  void submitDuplicateStateWhileCompletedIsRejected() throws Exception {
    stubExport(STATE_MISSING, 2);

    String firstJobId = manager.submitJob(STATE_MISSING);
    awaitStatus(firstJobId, JobStatus.COMPLETED, DEFAULT_AWAIT);

    assertThatThrownBy(() -> manager.submitJob(STATE_MISSING))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("already exists");
  }

  @Test
  void submitFailedStateDoesNotBlockRetry() throws Exception {
    when(schema.getUnhealthyContainersCount(any(), anyInt(), anyLong())).thenReturn(1L);
    when(schema.getUnhealthyContainersCursor(any(), anyInt(), anyLong()))
        .thenThrow(new RuntimeException("simulated DB failure"))
        .thenAnswer(inv -> finiteCursor(1));

    String firstJobId = manager.submitJob(STATE_MISSING);
    awaitStatus(firstJobId, JobStatus.FAILED, DEFAULT_AWAIT);

    String retryJobId = manager.submitJob(STATE_MISSING);
    awaitStatus(retryJobId, JobStatus.COMPLETED, DEFAULT_AWAIT);
    assertThat(retryJobId).isNotEqualTo(firstJobId);
  }

  @Test
  void submitQueueFullThrows() throws Exception {
    // Re-instantiate with the smallest possible queue (1) so we hit the
    // limit with the fewest distinct states. Layout once first job is
    // pulled by the worker:
    //   queue = [job2]                         (size 1, == maxQueueSize)
    //   submit job3 -> rejected
    manager.shutdown();
    manager = newManager(/* maxQueueSize */ 1);

    CountDownLatch release = new CountDownLatch(1);
    when(schema.getUnhealthyContainersCount(any(), anyInt(), anyLong())).thenReturn(1L);
    when(schema.getUnhealthyContainersCursor(any(), anyInt(), anyLong()))
        .thenAnswer(inv -> blockingCursor(release));

    String running = manager.submitJob(STATE_MISSING);
    awaitStatus(running, JobStatus.RUNNING, DEFAULT_AWAIT);
    String waiting = manager.submitJob(STATE_UNDER_REPLICATED);

    try {
      assertThatThrownBy(() -> manager.submitJob(STATE_OVER_REPLICATED))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("queue is full");
    } finally {
      release.countDown();
      awaitStatus(running, JobStatus.COMPLETED, DEFAULT_AWAIT);
      awaitStatus(waiting, JobStatus.COMPLETED, DEFAULT_AWAIT);
    }
  }

  // ── Cancel path ──────────────────────────────────────────────────────────

  @Test
  void cancelRunningJobMarksFailedAndRemovesFromTracker() throws Exception {
    CountDownLatch release = new CountDownLatch(1);
    when(schema.getUnhealthyContainersCount(any(), anyInt(), anyLong())).thenReturn(1L);
    when(schema.getUnhealthyContainersCursor(any(), anyInt(), anyLong()))
        .thenAnswer(inv -> blockingCursor(release));

    String jobId = manager.submitJob(STATE_MISSING);
    try {
      awaitStatus(jobId, JobStatus.RUNNING, DEFAULT_AWAIT);

      manager.cancelJob(jobId);

      assertThat(manager.getJob(jobId)).isNull();
      assertThat(new File(tempDir.toFile(), jobId)).doesNotExist();
    } finally {
      release.countDown();
    }
  }

  @Test
  void cancelCompletedJobDeletesTarFile() throws Exception {
    stubExport(STATE_MISSING, 2);

    String jobId = manager.submitJob(STATE_MISSING);
    awaitStatus(jobId, JobStatus.COMPLETED, DEFAULT_AWAIT);

    String tarPath = manager.getJob(jobId).getFilePath();
    assertThat(new File(tarPath)).exists();

    manager.cancelJob(jobId);

    assertThat(manager.getJob(jobId)).isNull();
    assertThat(new File(tarPath)).doesNotExist();
  }

  @Test
  void cancelUnknownJobIdThrows() {
    assertThatThrownBy(() -> manager.cancelJob("does-not-exist"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Job not found");
  }

  // ── Queue + listing ──────────────────────────────────────────────────────

  @Test
  void getQueuePositionAssignsSequentialPositions() throws Exception {
    CountDownLatch release = new CountDownLatch(1);
    when(schema.getUnhealthyContainersCount(any(), anyInt(), anyLong())).thenReturn(1L);
    when(schema.getUnhealthyContainersCursor(any(), anyInt(), anyLong()))
        .thenAnswer(inv -> blockingCursor(release));

    String running = manager.submitJob(STATE_MISSING);
    awaitStatus(running, JobStatus.RUNNING, DEFAULT_AWAIT);

    String waiting1 = manager.submitJob(STATE_UNDER_REPLICATED);
    String waiting2 = manager.submitJob(STATE_OVER_REPLICATED);

    try {
      assertThat(manager.getQueuePosition(running)).isZero();          // already running
      assertThat(manager.getQueuePosition(waiting1)).isEqualTo(1);
      assertThat(manager.getQueuePosition(waiting2)).isEqualTo(2);
    } finally {
      release.countDown();
      awaitStatus(running, JobStatus.COMPLETED, DEFAULT_AWAIT);
      awaitStatus(waiting1, JobStatus.COMPLETED, DEFAULT_AWAIT);
      awaitStatus(waiting2, JobStatus.COMPLETED, DEFAULT_AWAIT);
    }
  }

  @Test
  void getAllJobsReturnsEveryTrackedJob() throws Exception {
    stubExport(STATE_MISSING, 1);

    String first = manager.submitJob(STATE_MISSING);
    awaitStatus(first, JobStatus.COMPLETED, DEFAULT_AWAIT);

    String second = manager.submitJob(STATE_UNDER_REPLICATED);
    awaitStatus(second, JobStatus.COMPLETED, DEFAULT_AWAIT);

    List<ExportJob> jobs = manager.getAllJobs();
    assertThat(jobs).hasSize(2);
    assertThat(jobs).extracting(ExportJob::getJobId).containsExactlyInAnyOrder(first, second);
  }

  // ── Startup cleanup ──────────────────────────────────────────────────────

  @Test
  void startupCleanupRemovesLeftoverTarsAndDirs() throws Exception {
    manager.shutdown();

    File leftoverTar = new File(tempDir.toFile(), "stale_export.tar");
    Files.write(leftoverTar.toPath(), new byte[]{1, 2, 3});

    File leftoverDir = new File(tempDir.toFile(), "stale_job_id");
    Files.createDirectories(leftoverDir.toPath());
    Files.write(new File(leftoverDir, "part1.csv").toPath(), new byte[]{4, 5});

    File unrelated = new File(tempDir.toFile(), "keep.txt");
    Files.write(unrelated.toPath(), new byte[]{6});

    manager = newManager(/* maxQueueSize */ 4);

    assertThat(leftoverTar).doesNotExist();
    assertThat(leftoverDir).doesNotExist();
    assertThat(unrelated).exists();
  }

  // ── Filename derivation ─────────────────────────────────────────────────

  @Test
  void submitFileNameMatchesExpectedPattern() throws Exception {
    stubExport(STATE_MISSING, 1);

    String jobId = manager.submitJob(STATE_MISSING);
    awaitStatus(jobId, JobStatus.COMPLETED, DEFAULT_AWAIT);

    ExportJob job = manager.getJob(jobId);
    assertThat(job.getFileName()).matches("^export_missing_\\d+\\.tar$");
  }

  // ── Helpers ──────────────────────────────────────────────────────────────

  private ExportJobManager newManager(int maxQueueSize) {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_RECON_EXPORT_DIRECTORY, tempDir.toString());
    conf.setInt(OZONE_RECON_EXPORT_MAX_JOBS_TOTAL, maxQueueSize);
    conf.setInt(OZONE_RECON_EXPORT_MAX_DOWNLOADS, 3);
    return new ExportJobManager(schema, conf);
  }

  /**
   * Configures the schema mock so any export returns a finite cursor of
   * {@code recordCount} fake rows. Validates {@code state} eagerly so a bad
   * test fixture fails fast rather than from inside the worker thread.
   */
  private void stubExport(String state, int recordCount) {
    UnHealthyContainerStates.valueOf(state);
    when(schema.getUnhealthyContainersCount(any(), anyInt(), anyLong()))
        .thenReturn((long) recordCount);
    when(schema.getUnhealthyContainersCursor(any(), anyInt(), anyLong()))
        .thenAnswer(inv -> finiteCursor(recordCount));
  }

  /**
   * Returns a fresh mock cursor that yields {@code recordCount} fake records.
   */
  @SuppressWarnings("unchecked")
  private static Cursor<UnhealthyContainersRecord> finiteCursor(int recordCount) {
    Cursor<UnhealthyContainersRecord> cursor = mock(Cursor.class);
    AtomicInteger pos = new AtomicInteger(0);
    when(cursor.hasNext()).thenAnswer(inv -> pos.get() < recordCount);
    when(cursor.fetchNext()).thenAnswer(inv -> {
      int i = pos.getAndIncrement();
      UnhealthyContainersRecord rec = new UnhealthyContainersRecord();
      rec.setContainerId((long) (i + 1));
      rec.setContainerState("MISSING");
      rec.setInStateSince(0L);
      rec.setExpectedReplicaCount(3);
      rec.setActualReplicaCount(2);
      rec.setReplicaDelta(1);
      return rec;
    });
    return cursor;
  }

  /**
   * Returns a fresh mock cursor whose {@code hasNext()} blocks until the
   * provided latch is counted down. Used to keep a running job suspended
   * mid-execution so the test can observe RUNNING status. If the worker
   * thread is interrupted (e.g. by {@code cancelJob}) we propagate via an
   * unchecked exception so the manager's outer {@code catch (Exception)}
   * runs the failure / cleanup path.
   */
  @SuppressWarnings("unchecked")
  private static Cursor<UnhealthyContainersRecord> blockingCursor(CountDownLatch release) {
    Cursor<UnhealthyContainersRecord> cursor = mock(Cursor.class);
    when(cursor.hasNext()).thenAnswer(inv -> {
      try {
        release.await();
        return false;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("blockingCursor interrupted", e);
      }
    });
    return cursor;
  }

  private static List<String> listTarEntryNames(File tarFile) throws Exception {
    List<String> entries = new ArrayList<>();
    try (TarArchiveInputStream tis =
             new TarArchiveInputStream(Files.newInputStream(tarFile.toPath()))) {
      TarArchiveEntry entry;
      while ((entry = tis.getNextTarEntry()) != null) {
        if (!entry.isDirectory()) {
          entries.add(entry.getName());
        }
      }
    }
    return entries;
  }

  private void awaitStatus(String jobId, JobStatus target, Duration timeout) throws Exception {
    long deadlineNanos = System.nanoTime() + timeout.toNanos();
    while (System.nanoTime() < deadlineNanos) {
      ExportJob job = manager.getJob(jobId);
      if (job != null && job.getStatus() == target) {
        return;
      }
      TimeUnit.MILLISECONDS.sleep(50);
    }
    ExportJob latest = manager.getJob(jobId);
    throw new AssertionError("Timed out waiting for job " + jobId + " to reach " + target
        + "; last observed=" + (latest == null ? "<absent>" : latest.getStatus()));
  }
}
