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

package org.apache.hadoop.ozone.recon.persistence;

import static org.apache.ozone.recon.schema.generated.tables.UnhealthyContainersTable.UNHEALTHY_CONTAINERS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.inject.Injector;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.recon.persistence.ContainerHealthSchemaManager.ContainerStateKey;
import org.apache.hadoop.ozone.recon.persistence.ContainerHealthSchemaManager.UnhealthyContainerRecord;
import org.apache.ozone.recon.schema.ContainerSchemaDefinition;
import org.apache.ozone.recon.schema.ContainerSchemaDefinition.UnHealthyContainerStates;
import org.jooq.DSLContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Measures {@link ContainerHealthSchemaManager#syncUnhealthyContainerRecordsAtomically}
 * against a <b>file-based</b> Derby database, which is the storage mode Recon
 * actually uses in production (the sibling {@code TestUnhealthyContainersDerbyPerformance}
 * runs in-memory to isolate engine cost).
 *
 * <p>The scenario mirrors one steady-state health-scan chunk rather than a
 * worst-case full rewrite: {@value #CHUNK_CONTAINERS} containers are already
 * tracked (one production {@code PERSIST_CHUNK_SIZE}), and the current scan
 * leaves most of them unchanged. Only a small fraction changed replica counts
 * or recovered, so sync should skip the unchanged majority and issue writes for
 * just the churn. This is the case the change-detection + batching work is meant
 * to make cheap on disk.</p>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestUnhealthyContainersFileBasedSyncBenchmark {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestUnhealthyContainersFileBasedSyncBenchmark.class);

  /** One production PERSIST_CHUNK_SIZE worth of already-tracked unhealthy containers. */
  private static final int CHUNK_CONTAINERS = 50_000;

  /** Rows committed per seed transaction, bounding Derby's write-ahead log growth. */
  private static final int SEED_TX_SIZE = 2_000;

  private static final long IN_STATE_SINCE = 1_000L;
  private static final int EXPECTED_REPLICAS = 3;
  private static final int HEALTHY_ACTUAL = 2;
  private static final int DEGRADED_ACTUAL = 1;
  private static final String REASON = "Insufficient replicas";

  /** Generous CI-safe ceiling; a steady-state chunk should finish in a few seconds. */
  private static final long MAX_SYNC_SECONDS = 120;

  private ContainerHealthSchemaManager schemaManager;
  private ContainerSchemaDefinition schemaDefinition;
  private String jdbcUrl;

  @BeforeAll
  public void setUp(@TempDir Path tempDir) throws Exception {
    jdbcUrl = "jdbc:derby:" + tempDir.resolve("reconSyncDb").toAbsolutePath() + ";create=true";
    LOG.info("=== File-based Derby sync benchmark — Setup ({}) ===", jdbcUrl);

    Injector injector =
        TestUnhealthyContainersDerbyPerformance.createDerbyInjector(tempDir, jdbcUrl);
    schemaDefinition = injector.getInstance(ContainerSchemaDefinition.class);
    schemaManager = new ContainerHealthSchemaManager(schemaDefinition, new OzoneConfiguration());

    seedTrackedContainers();
  }

  @AfterAll
  public void tearDown() {
    TestUnhealthyContainersDerbyPerformance.teardownDerbyDatabase(jdbcUrl, LOG);
  }

  @Test
  public void benchmarkSteadyStateSyncOnFileBasedDerby() {
    List<Long> containerIds = new ArrayList<>(CHUNK_CONTAINERS);
    for (long id = 1; id <= CHUNK_CONTAINERS; id++) {
      containerIds.add(id);
    }

    // Production loads existing rows once per chunk before syncing.
    Map<ContainerStateKey, UnhealthyContainerRecord> existing =
        schemaManager.getExistingUnhealthyRecordsByContainerIds(containerIds);
    assertEquals(CHUNK_CONTAINERS, existing.size(),
        "All seeded containers should be loaded as existing rows");

    // Build this scan's result with realistic churn keyed off container id:
    //   id % 100 in {0,1,2} -> recovered (omitted -> stale delete)  ~3%
    //   id % 100 in [3,10)  -> changed replica count (-> update)    ~7%
    //   otherwise           -> unchanged (identical -> skipped)     ~90%
    List<UnhealthyContainerRecord> desired = new ArrayList<>();
    int recovered = 0;
    int changed = 0;
    int unchanged = 0;
    for (long id = 1; id <= CHUNK_CONTAINERS; id++) {
      long bucket = id % 100;
      if (bucket < 3) {
        recovered++;
      } else if (bucket < 10) {
        desired.add(record(id, DEGRADED_ACTUAL));
        changed++;
      } else {
        desired.add(record(id, HEALTHY_ACTUAL));
        unchanged++;
      }
    }

    long start = System.nanoTime();
    schemaManager.syncUnhealthyContainerRecordsAtomically(existing, desired);
    long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

    LOG.info("File-based steady-state sync: {} tracked, {} unchanged (skipped), "
            + "{} updated, {} recovered (deleted) in {} ms",
        CHUNK_CONTAINERS, unchanged, changed, recovered, elapsedMs);

    assertTrue(elapsedMs <= TimeUnit.SECONDS.toMillis(MAX_SYNC_SECONDS),
        String.format("Steady-state sync took %d ms, exceeded %d s threshold",
            elapsedMs, MAX_SYNC_SECONDS));

    DSLContext dsl = schemaDefinition.getDSLContext();
    assertEquals(CHUNK_CONTAINERS - recovered, dsl.fetchCount(UNHEALTHY_CONTAINERS),
        "Only recovered containers should have been deleted");
    assertFalse(rowExists(dsl, 100L), "Recovered container 100 should be deleted");
    assertEquals(DEGRADED_ACTUAL, actualReplicaCount(dsl, 5L),
        "Changed container 5 should carry the updated replica count");
    assertEquals(HEALTHY_ACTUAL, actualReplicaCount(dsl, 50L),
        "Unchanged container 50 should retain its original replica count");
  }

  private void seedTrackedContainers() {
    List<UnhealthyContainerRecord> batch = new ArrayList<>(SEED_TX_SIZE);
    for (long id = 1; id <= CHUNK_CONTAINERS; id++) {
      batch.add(record(id, HEALTHY_ACTUAL));
      if (batch.size() == SEED_TX_SIZE) {
        schemaManager.insertUnhealthyContainerRecords(batch);
        batch.clear();
      }
    }
    if (!batch.isEmpty()) {
      schemaManager.insertUnhealthyContainerRecords(batch);
    }
  }

  private UnhealthyContainerRecord record(long id, int actualReplicas) {
    return new UnhealthyContainerRecord(id,
        UnHealthyContainerStates.UNDER_REPLICATED.toString(),
        IN_STATE_SINCE, EXPECTED_REPLICAS, actualReplicas,
        EXPECTED_REPLICAS - actualReplicas, REASON);
  }

  private boolean rowExists(DSLContext dsl, long id) {
    return dsl.fetchExists(dsl.selectFrom(UNHEALTHY_CONTAINERS)
        .where(UNHEALTHY_CONTAINERS.CONTAINER_ID.eq(id)));
  }

  private int actualReplicaCount(DSLContext dsl, long id) {
    return dsl.select(UNHEALTHY_CONTAINERS.ACTUAL_REPLICA_COUNT)
        .from(UNHEALTHY_CONTAINERS)
        .where(UNHEALTHY_CONTAINERS.CONTAINER_ID.eq(id))
        .fetchOne(UNHEALTHY_CONTAINERS.ACTUAL_REPLICA_COUNT);
  }
}
