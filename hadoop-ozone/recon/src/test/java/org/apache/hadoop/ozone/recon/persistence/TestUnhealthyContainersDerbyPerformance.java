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
import static org.jooq.impl.DSL.count;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provider;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.ozone.recon.ReconControllerModule.ReconDaoBindingModule;
import org.apache.hadoop.ozone.recon.ReconSchemaManager;
import org.apache.hadoop.ozone.recon.persistence.AbstractReconSqlDBTest.DerbyDataSourceConfigurationProvider;
import org.apache.hadoop.ozone.recon.persistence.ContainerHealthSchemaManager.UnhealthyContainerRecord;
import org.apache.hadoop.ozone.recon.persistence.ContainerHealthSchemaManager.UnhealthyContainersSummary;
import org.apache.ozone.recon.schema.ContainerSchemaDefinition;
import org.apache.ozone.recon.schema.ContainerSchemaDefinition.UnHealthyContainerStates;
import org.apache.ozone.recon.schema.ReconSchemaGenerationModule;
import org.apache.ozone.recon.schema.generated.tables.daos.UnhealthyContainersDao;
import org.jooq.DSLContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Performance benchmark for the UNHEALTHY_CONTAINERS Derby table at 1 million
 * records scale.
 *
 * <h2>Data layout</h2>
 * <pre>
 *   Container IDs  : 1 – 200,000  (CONTAINER_ID_RANGE)
 *   States per ID  : 5  (UNDER_REPLICATED, MISSING, OVER_REPLICATED,
 *                        MIS_REPLICATED, EMPTY_MISSING)
 *   Total records  : 200,000 × 5 = 1,000,000
 *   Primary key    : (container_id, container_state)  — unique per pair
 *   Index          : idx_state_container_id on (container_state, container_id)
 *                    composite index supports both aggregates (COUNT/GROUP BY
 *                    on state prefix) and O(1)-per-page cursor pagination
 * </pre>
 *
 * <h2>Performance settings applied in this test</h2>
 * <ul>
 *   <li><b>Page cache</b>: {@code derby.storage.pageCacheSize = 20000}
 *       (~80 MB of 4-KB pages) keeps hot B-tree nodes in memory, reducing
 *       filesystem reads even with the file-based Derby driver.</li>
 *   <li><b>JDBC fetch size</b>: set to {@value #READ_PAGE_SIZE} on each query
 *       so Derby pre-buffers a full page of rows per JDBC round-trip instead
 *       of the default 1-row-at-a-time fetch.</li>
 *   <li><b>Large page size</b>: {@value #READ_PAGE_SIZE} rows per SQL fetch
 *       reduces the number of SQL round-trips from 200 (@ 1 K rows) to 40
 *       (@ 5 K rows) per 200 K-row state scan.</li>
 *   <li><b>Large delete chunks</b>: {@value #DELETE_CHUNK_SIZE} IDs per
 *       DELETE statement reduces Derby plan-compilation overhead from 100
 *       statements to 20 for a 100 K-ID batch delete.</li>
 * </ul>
 *
 * <h2>What is measured</h2>
 * <ol>
 *   <li><b>Bulk INSERT throughput</b> – 1 M records via JOOQ batchInsert in
 *       chunks of 1,000 inside a single Derby transaction.</li>
 *   <li><b>COUNT(*) by state</b> – index-covered aggregate, one per state.</li>
 *   <li><b>GROUP BY summary</b> – single pass over the idx_container_state
 *       index to aggregate all states.</li>
 *   <li><b>Paginated SELECT by state</b> – cursor-style walk using
 *       minContainerId / maxContainerId to fetch the full 200 K rows of one
 *       state in pages of {@value #READ_PAGE_SIZE}, without loading all rows
 *       into the JVM heap at once.</li>
 *   <li><b>Batch DELETE throughput</b> – removes records for half the
 *       container IDs list covering all rows
 *       (200 K × 5 states = 1 M rows) via a single
 *       IN-clause DELETE.</li>
 * </ol>
 *
 * <h2>Design notes</h2>
 * <ul>
 *   <li>Derby is an embedded, single-file Java database — not designed for
 *       production-scale workloads. Performance numbers here document its
 *       baseline behaviour and will flag regressions, but should not be
 *       compared with PostgreSQL / MySQL numbers.</li>
 *   <li>Timing thresholds are deliberately generous (≈ 10× expected) to be
 *       stable on slow CI machines. Actual durations are always logged.</li>
 *   <li>Uses {@code @TestInstance(PER_CLASS)} so database/schema setup is
 *       done once in {@code @BeforeAll}; test methods then exercise
 *       insert/replace/delete flows explicitly.</li>
 * </ul>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TestUnhealthyContainersDerbyPerformance {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestUnhealthyContainersDerbyPerformance.class);

  // -----------------------------------------------------------------------
  // Dataset constants
  // -----------------------------------------------------------------------

  /** Number of unique container IDs.  Each ID appears in every TESTED_STATES. */
  private static final int CONTAINER_ID_RANGE = 200_000;

  /** States distributed across all container IDs. */
  private static final List<UnHealthyContainerStates> TESTED_STATES = Arrays.asList(
      UnHealthyContainerStates.UNDER_REPLICATED,
      UnHealthyContainerStates.MISSING,
      UnHealthyContainerStates.OVER_REPLICATED,
      UnHealthyContainerStates.MIS_REPLICATED,
      UnHealthyContainerStates.EMPTY_MISSING);

  /** Number of tested states (equals TESTED_STATES.size()). */
  private static final int STATE_COUNT = 5;

  /** Total records = CONTAINER_ID_RANGE × STATE_COUNT. */
  private static final int TOTAL_RECORDS = CONTAINER_ID_RANGE * STATE_COUNT;

  /**
   * Number of containers inserted per transaction.
   *
   * <p>Derby's WAL (Write-Ahead Log) must hold all uncommitted rows before
   * a transaction commits.  Inserting all 1 M rows in one transaction causes
   * Derby to exhaust its log buffer and hang indefinitely.  Committing in
   * chunks of {@value} containers ({@value} × 5 states = 10,000 rows/tx)
   * lets Derby flush the log after each commit, keeping each transaction
   * fast and bounded in memory usage.</p>
   */
  private static final int CONTAINERS_PER_TX = 2_000;   // 2 000 × 5 = 10 000 rows/tx

  /**
   * Number of container IDs to pass per
   * {@link ContainerHealthSchemaManager#batchDeleteSCMStatesForContainers}
   * call in the delete test.
   *
   * <p>{@code batchDeleteSCMStatesForContainers} now handles internal
   * chunking at 1,000 IDs per SQL statement to stay within Derby's
   * 64 KB generated-bytecode limit
   * (ERROR XBCM4).  This test-level constant controls how many IDs are
   * accumulated before each call and should match that limit so the test
   * exercises exactly one SQL DELETE per call.</p>
   */
  private static final int DELETE_CHUNK_SIZE = 1_000;

  /**
   * Number of records returned per page in the paginated-read tests.
   *
   * <p>5,000 rows per page means only 40 SQL round-trips to scan 200,000
   * records for a single state, compared to 200 trips at the old 1,000-row
   * page size.  Combined with {@code query.fetchSize(READ_PAGE_SIZE)} this
   * cuts round-trip overhead by 80% while keeping per-page heap usage well
   * below 1 MB.</p>
   */
  private static final int READ_PAGE_SIZE = 5_000;

  // -----------------------------------------------------------------------
  // Performance thresholds (CI-safe; expected run times are 5–10× faster
  // than the original file-based Derby baseline after the optimisations)
  // -----------------------------------------------------------------------

  /** Maximum acceptable time to insert all TOTAL_RECORDS into Derby. */
  private static final long MAX_INSERT_SECONDS = 300;

  /** Maximum acceptable time for a single COUNT(*)-by-state query. */
  private static final long MAX_COUNT_BY_STATE_SECONDS = 30;

  /** Maximum acceptable time for the GROUP-BY summary query. */
  private static final long MAX_SUMMARY_SECONDS = 30;

  /**
   * Maximum acceptable time to page through all CONTAINER_ID_RANGE records
   * of a single state using {@link #READ_PAGE_SIZE}-row pages.
   */
  private static final long MAX_PAGINATED_READ_SECONDS = 60;

  /** Maximum acceptable time to batch-delete 1 M rows. */
  private static final long MAX_DELETE_SECONDS = 180;
  /** Maximum acceptable time for one atomic delete+insert replace cycle. */
  private static final long MAX_ATOMIC_REPLACE_SECONDS = 300;

  // -----------------------------------------------------------------------
  // Infrastructure (shared for the life of this test class)
  // -----------------------------------------------------------------------

  private ContainerHealthSchemaManager schemaManager;
  private UnhealthyContainersDao dao;
  private ContainerSchemaDefinition schemaDefinition;

  // -----------------------------------------------------------------------
  // One-time setup: create Derby schema + insert 1 M records
  // -----------------------------------------------------------------------

  /**
   * Initialises the embedded Derby database and creates the Recon schema.
   * Data population is done in dedicated test methods.
   *
   * <p>The {@code @TempDir} is injected as a <em>method parameter</em> rather
   * than a class field.  With {@code @TestInstance(PER_CLASS)}, a field-level
   * {@code @TempDir} is populated by JUnit's {@code TempDirExtension} in its
   * own {@code beforeAll} callback, which may run <em>after</em> the user's
   * {@code @BeforeAll} — leaving it null when needed here.  A method
   * parameter is resolved by JUnit before the method body executes.</p>
   *
   * <h3>Performance settings applied here</h3>
   * <ul>
   *   <li><b>Page cache</b> ({@code derby.storage.pageCacheSize = 20000}):
   *       ~80 MB of 4-KB B-tree pages resident in heap — covers the hot path
   *       for index scans on a 1-M-row table even with the file-based
   *       driver.</li>
   * </ul>
   */
  @BeforeAll
  public void setUpDatabase(@TempDir Path tempDir) throws Exception {
    LOG.info("=== Derby Performance Benchmark — Setup ===");
    LOG.info("Dataset: {} states × {} container IDs = {} total records",
        TESTED_STATES.size(), CONTAINER_ID_RANGE, TOTAL_RECORDS);

    // Derby engine property — must be set before the first connection.
    //
    // pageCacheSize: number of 4-KB pages Derby keeps in its buffer pool.
    //   Default = 1,000 pages (4 MB) — far too small for a 1-M-row table.
    //   20,000 pages = ~80 MB, enough to hold the full B-tree for both the
    //   primary-key index and the composite (state, container_id) index.
    System.setProperty("derby.storage.pageCacheSize", "20000");

    // ----- Guice wiring (mirrors AbstractReconSqlDBTest) -----
    File configDir = Files.createDirectory(tempDir.resolve("Config")).toFile();
    Provider<DataSourceConfiguration> configProvider =
        new DerbyDataSourceConfigurationProvider(configDir);

    Injector injector = Guice.createInjector(
        new JooqPersistenceModule(configProvider),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(DataSourceConfiguration.class).toProvider(configProvider);
            bind(ReconSchemaManager.class);
          }
        },
        new ReconSchemaGenerationModule(),
        new ReconDaoBindingModule());

    injector.getInstance(ReconSchemaManager.class).createReconSchema();

    dao = injector.getInstance(UnhealthyContainersDao.class);
    schemaDefinition = injector.getInstance(ContainerSchemaDefinition.class);
    schemaManager = new ContainerHealthSchemaManager(schemaDefinition);
  }

  // -----------------------------------------------------------------------
  // Test 1 — Batch INSERT performance for 1M records
  // -----------------------------------------------------------------------

  /**
   * Inserts 1M records via batch operations and logs total time taken.
   */
  @Test
  @Order(1)
  public void testBatchInsertOneMillionRecords() {
    int txCount = (int) Math.ceil((double) CONTAINER_ID_RANGE / CONTAINERS_PER_TX);
    LOG.info("--- Test 1: Batch INSERT {} records ({} containers/tx, {} transactions) ---",
        TOTAL_RECORDS, CONTAINERS_PER_TX, txCount);

    long now = System.currentTimeMillis();
    long start = System.nanoTime();

    for (int startId = 1; startId <= CONTAINER_ID_RANGE; startId += CONTAINERS_PER_TX) {
      int endId = Math.min(startId + CONTAINERS_PER_TX - 1, CONTAINER_ID_RANGE);
      List<UnhealthyContainerRecord> chunk = generateRecordsForRange(startId, endId, now);
      schemaManager.insertUnhealthyContainerRecords(chunk);
    }

    long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
    double throughput = (double) TOTAL_RECORDS / (elapsedMs / 1000.0);
    LOG.info("Batch INSERT complete: {} records in {} ms ({} rec/sec, {} tx)",
        TOTAL_RECORDS, elapsedMs, String.format("%.0f", throughput), txCount);

    assertTrue(elapsedMs <= TimeUnit.SECONDS.toMillis(MAX_INSERT_SECONDS),
        String.format("INSERT took %d ms, exceeded %d s threshold",
            elapsedMs, MAX_INSERT_SECONDS));
  }

  // -----------------------------------------------------------------------
  // Test 2 — Verify the inserted row count
  // -----------------------------------------------------------------------

  @Test
  @Order(2)
  public void testTotalInsertedRecordCountIsOneMillion() {
    LOG.info("--- Test 2: Verify total row count = {} ---", TOTAL_RECORDS);

    long countStart = System.nanoTime();
    long totalCount = dao.count();
    long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - countStart);

    LOG.info("COUNT(*) = {} rows in {} ms", totalCount, elapsedMs);

    assertEquals(TOTAL_RECORDS, totalCount,
        "Total row count must equal the number of inserted records");
  }

  // -----------------------------------------------------------------------
  // Test 2 — COUNT(*) by each state (exercises idx_container_state)
  // -----------------------------------------------------------------------

  /**
   * Runs one {@code COUNT(*) WHERE container_state = ?} query per tested
   * state.  Because {@code container_state} is indexed these should be fast
   * index-covered aggregates.
   *
   * <p>Each state must have exactly {@value #CONTAINER_ID_RANGE} records.</p>
   */
  @Test
  @Order(3)
  public void testCountByStatePerformanceUsesIndex() {
    LOG.info("--- Test 3: COUNT(*) by state (index-covered, {} records each) ---",
        CONTAINER_ID_RANGE);

    DSLContext dsl = schemaDefinition.getDSLContext();

    for (UnHealthyContainerStates state : TESTED_STATES) {
      long start = System.nanoTime();
      int stateCount = dsl
          .select(count())
          .from(UNHEALTHY_CONTAINERS)
          .where(UNHEALTHY_CONTAINERS.CONTAINER_STATE.eq(state.toString()))
          .fetchOne(0, int.class);
      long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

      LOG.info("  COUNT({}) = {} rows  in {} ms", state, stateCount, elapsedMs);

      assertEquals(CONTAINER_ID_RANGE, stateCount,
          "Expected " + CONTAINER_ID_RANGE + " records for state " + state);

      assertTrue(elapsedMs <= TimeUnit.SECONDS.toMillis(MAX_COUNT_BY_STATE_SECONDS),
          String.format("COUNT for state %s took %d ms, exceeded %d s threshold",
              state, elapsedMs, MAX_COUNT_BY_STATE_SECONDS));
    }
  }

  // -----------------------------------------------------------------------
  // Test 3 — GROUP BY summary query
  // -----------------------------------------------------------------------

  /**
   * Runs the {@link ContainerHealthSchemaManager#getUnhealthyContainersSummary()}
   * GROUP-BY query over all 1 M rows, which represents a typical API request
   * to populate the Recon UI dashboard.
   *
   * <p>Expected result: {@value #STATE_COUNT} state groups, each with
   * {@value #CONTAINER_ID_RANGE} records.</p>
   */
  @Test
  @Order(4)
  public void testGroupBySummaryQueryPerformance() {
    LOG.info("--- Test 4: GROUP BY summary over {} rows ---", TOTAL_RECORDS);

    long start = System.nanoTime();
    List<UnhealthyContainersSummary> summary =
        schemaManager.getUnhealthyContainersSummary();
    long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

    LOG.info("GROUP BY summary: {} state groups returned in {} ms",
        summary.size(), elapsedMs);
    summary.forEach(s ->
        LOG.info("  state={} count={}", s.getContainerState(), s.getCount()));

    assertEquals(STATE_COUNT, summary.size(),
        "Summary must contain one entry per tested state");

    for (UnhealthyContainersSummary entry : summary) {
      assertEquals(CONTAINER_ID_RANGE, entry.getCount(),
          "Each state must have " + CONTAINER_ID_RANGE + " records in the summary");
    }

    assertTrue(elapsedMs <= TimeUnit.SECONDS.toMillis(MAX_SUMMARY_SECONDS),
        String.format("GROUP BY took %d ms, exceeded %d s threshold",
            elapsedMs, MAX_SUMMARY_SECONDS));
  }

  // -----------------------------------------------------------------------
  // Test 4 — Paginated read (cursor walk through 200 K rows per state)
  // -----------------------------------------------------------------------

  /**
   * Reads all {@value #CONTAINER_ID_RANGE} records of one state
   * ({@code UNDER_REPLICATED}) by walking through them page-by-page using
   * the {@code minContainerId} cursor parameter.  This simulates the Recon
   * UI pagination pattern without holding the full result-set in heap memory.
   *
   * <p>The test asserts:</p>
   * <ul>
   *   <li>Total records seen across all pages equals {@value #CONTAINER_ID_RANGE}</li>
   *   <li>All pages are fetched within {@value #MAX_PAGINATED_READ_SECONDS} seconds</li>
   *   <li>Records are returned in ascending container-ID order</li>
   * </ul>
   */
  @Test
  @Order(5)
  public void testPaginatedReadByStatePerformance() {
    UnHealthyContainerStates targetState = UnHealthyContainerStates.UNDER_REPLICATED;
    LOG.info("--- Test 5: Paginated read of {} ({} records, page size {}) ---",
        targetState, CONTAINER_ID_RANGE, READ_PAGE_SIZE);

    int totalRead = 0;
    int pageCount = 0;
    long minContainerId = 0;
    long lastContainerId = -1;
    boolean orderedCorrectly = true;

    long start = System.nanoTime();

    while (true) {
      List<ContainerHealthSchemaManager.UnhealthyContainerRecord> page =
          schemaManager.getUnhealthyContainers(
              targetState, minContainerId, 0, READ_PAGE_SIZE);

      if (page.isEmpty()) {
        break;
      }

      for (ContainerHealthSchemaManager.UnhealthyContainerRecord rec : page) {
        if (rec.getContainerId() <= lastContainerId) {
          orderedCorrectly = false;
        }
        lastContainerId = rec.getContainerId();
      }

      totalRead += page.size();
      pageCount++;
      minContainerId = page.get(page.size() - 1).getContainerId();

      if (page.size() < READ_PAGE_SIZE) {
        break;
      }
    }

    long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
    double throughput = totalRead / Math.max(1.0, elapsedMs / 1000.0);

    LOG.info("Paginated read: {} records in {} pages, {} ms  ({} rec/sec)",
        totalRead, pageCount, elapsedMs, String.format("%.0f", throughput));

    assertEquals(CONTAINER_ID_RANGE, totalRead,
        "Paginated read must return all " + CONTAINER_ID_RANGE + " records for " + targetState);

    assertTrue(orderedCorrectly,
        "Records must be returned in ascending container_id order");

    assertTrue(elapsedMs <= TimeUnit.SECONDS.toMillis(MAX_PAGINATED_READ_SECONDS),
        String.format("Paginated read took %d ms, exceeded %d s threshold",
            elapsedMs, MAX_PAGINATED_READ_SECONDS));
  }

  // -----------------------------------------------------------------------
  // Test 5 — Read all states sequentially (full 1 M record scan via pages)
  // -----------------------------------------------------------------------

  /**
   * Pages through all records for every tested state sequentially, effectively
   * reading all 1 million rows from Derby through the application layer.
   * This measures aggregate read throughput across the entire dataset.
   */
  @Test
  @Order(6)
  public void testFullDatasetReadThroughputAllStates() {
    LOG.info("--- Test 6: Full {} M record read (all states, paged) ---",
        TOTAL_RECORDS / 1_000_000);

    long totalStart = System.nanoTime();
    Map<UnHealthyContainerStates, Integer> countPerState =
        new EnumMap<>(UnHealthyContainerStates.class);

    for (UnHealthyContainerStates state : TESTED_STATES) {
      long stateStart = System.nanoTime();
      int stateTotal = 0;
      long minId = 0;

      while (true) {
        List<ContainerHealthSchemaManager.UnhealthyContainerRecord> page =
            schemaManager.getUnhealthyContainers(state, minId, 0, READ_PAGE_SIZE);
        if (page.isEmpty()) {
          break;
        }
        stateTotal += page.size();
        minId = page.get(page.size() - 1).getContainerId();
        if (page.size() < READ_PAGE_SIZE) {
          break;
        }
      }

      long stateElapsedMs =
          TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - stateStart);
      countPerState.put(state, stateTotal);
      LOG.info("  State {}: {} records in {} ms", state, stateTotal, stateElapsedMs);
    }

    long totalElapsedMs =
        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - totalStart);
    int grandTotal = countPerState.values().stream().mapToInt(Integer::intValue).sum();
    double overallThroughput = grandTotal / Math.max(1.0, totalElapsedMs / 1000.0);

    LOG.info("Full dataset read: {} total records in {} ms  ({} rec/sec)",
        grandTotal, totalElapsedMs, String.format("%.0f", overallThroughput));

    assertEquals(TOTAL_RECORDS, grandTotal,
        "Full dataset read must return exactly " + TOTAL_RECORDS + " records");

    countPerState.forEach((state, cnt) ->
        assertEquals(CONTAINER_ID_RANGE, cnt,
            "State " + state + " must have " + CONTAINER_ID_RANGE + " records"));
  }

  // -----------------------------------------------------------------------
  // Test 7 — Atomic replace (delete + insert) performance for 1M records
  // -----------------------------------------------------------------------

  /**
   * Exercises the same persistence pattern used by Recon health scan chunks:
   * delete and insert in a single transaction.
   *
   * <p>This validates that {@link ContainerHealthSchemaManager#replaceUnhealthyContainerRecordsAtomically}
   * can safely replace a large chunk without changing total row count and
   * that rewritten records are visible with the new timestamp.</p>
   */
  @Test
  @Order(7)
  public void testAtomicReplaceDeleteAndInsertInSingleTransaction() {
    int replaceContainerCount = CONTAINER_ID_RANGE;
    long replacementTimestamp = System.currentTimeMillis() + 10_000;
    int expectedRowsReplaced = replaceContainerCount * STATE_COUNT;

    LOG.info("--- Test 7: Atomic replace — {} IDs × {} states = {} rows in one tx ---",
        replaceContainerCount, STATE_COUNT, expectedRowsReplaced);

    List<Long> idsToReplace = new ArrayList<>(replaceContainerCount);
    for (long id = 1; id <= replaceContainerCount; id++) {
      idsToReplace.add(id);
    }
    List<UnhealthyContainerRecord> replacementRecords =
        generateRecordsForRange(1, replaceContainerCount, replacementTimestamp);

    long start = System.nanoTime();
    schemaManager.replaceUnhealthyContainerRecordsAtomically(idsToReplace, replacementRecords);
    long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
    LOG.info("Atomic replace completed in {} ms", elapsedMs);

    assertTrue(elapsedMs <= TimeUnit.SECONDS.toMillis(MAX_ATOMIC_REPLACE_SECONDS),
        String.format("Atomic replace took %d ms, exceeded %d s threshold",
            elapsedMs, MAX_ATOMIC_REPLACE_SECONDS));

    long totalCount = dao.count();
    assertEquals(TOTAL_RECORDS, totalCount,
        "Atomic replace should not change total row count");

    List<ContainerHealthSchemaManager.UnhealthyContainerRecord> firstPage =
        schemaManager.getUnhealthyContainers(
            UnHealthyContainerStates.UNDER_REPLICATED, 0, 0, 1);
    assertEquals(1, firstPage.size(), "Expected first under-replicated row");
    assertEquals(1L, firstPage.get(0).getContainerId(),
        "Expected containerId=1 as first row for UNDER_REPLICATED");
    assertEquals(replacementTimestamp, firstPage.get(0).getInStateSince(),
        "Replaced rows should carry the replacement timestamp");
  }

  // -----------------------------------------------------------------------
  // Test 8 — Large IN-clause read must be internally chunked
  // -----------------------------------------------------------------------

  /**
   * Verifies that loading existing in-state-since values for a large set of
   * container IDs does not generate a single oversized Derby statement.
   *
   * <p>This regression test covers the read path used by
   * {@link org.apache.hadoop.ozone.recon.fsck.ContainerHealthTask} while it
   * preserves {@code in_state_since} values across scan cycles. Before
   * internal chunking, passing a large ID list here caused Derby to fail with
   * {@code ERROR 42ZA0: Statement too complex} and
   * {@code constant_pool > 65535} during statement compilation.</p>
   */
  @Test
  @Order(8)
  public void testExistingInStateSinceLookupChunksLargeContainerIdList() {
    int lookupCount = 20_000;
    int expectedRecords = lookupCount * STATE_COUNT;
    List<Long> containerIds = new ArrayList<>(lookupCount);

    for (long id = 1; id <= lookupCount; id++) {
      containerIds.add(id);
    }

    long start = System.nanoTime();
    Map<ContainerHealthSchemaManager.ContainerStateKey, Long> existing =
        schemaManager.getExistingInStateSinceByContainerIds(containerIds);
    long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

    LOG.info("Large in-state-since lookup complete: {} container IDs -> {} rows in {} ms",
        lookupCount, existing.size(), elapsedMs);

    assertEquals(expectedRecords, existing.size(),
        "Lookup should return one record per existing container/state pair");
  }

  // -----------------------------------------------------------------------
  // Test 9 — Batch DELETE performance for 1M records
  // -----------------------------------------------------------------------

  /**
   * Deletes records for all container IDs (1 – 200,000) across
   * all five states by passing the complete ID list in one call to
   * {@link ContainerHealthSchemaManager#batchDeleteSCMStatesForContainers}.
   *
   * <p>{@code batchDeleteSCMStatesForContainers} now handles internal
   * chunking at {@value #DELETE_CHUNK_SIZE}
   * IDs per SQL statement to stay within Derby's 64 KB generated-bytecode
   * limit (JVM ERROR XBCM4).  Passing 100 K IDs in a single call is safe
   * because the method partitions them internally into 200 statements of
   * 1,000 IDs each — matching Recon's real scan-cycle pattern for large
   * clusters.</p>
   *
   * <p>Expected outcome: 200 K × 5 states = 1 M rows deleted, 0 remain.</p>
   *
   * <p><b>Note:</b> this test modifies the shared dataset, so it runs after
   * all read-only tests.</p>
   */
  @Test
  @Order(9)
  public void testBatchDeletePerformanceOneMillionRecords() {
    int deleteCount = CONTAINER_ID_RANGE;               // 200 000 container IDs
    int expectedDeleted = deleteCount * STATE_COUNT;    // 1 000 000 rows
    int expectedRemaining = TOTAL_RECORDS - expectedDeleted;
    int internalChunks = (int) Math.ceil(
        (double) deleteCount / DELETE_CHUNK_SIZE);

    LOG.info("--- Test 9: Batch DELETE — {} IDs × {} states = {} rows  "
            + "({} internal SQL statements of {} IDs) ---",
        deleteCount, STATE_COUNT, expectedDeleted,
        internalChunks, DELETE_CHUNK_SIZE);

    long start = System.nanoTime();

    // Build the full list of container IDs to delete and pass in one call.
    // batchDeleteSCMStatesForContainers partitions them internally so the
    // caller does not need to chunk manually.
    List<Long> idsToDelete = new ArrayList<>(deleteCount);
    for (long id = 1; id <= deleteCount; id++) {
      idsToDelete.add(id);
    }
    schemaManager.batchDeleteSCMStatesForContainers(idsToDelete);

    long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
    double deleteThroughput = expectedDeleted / Math.max(1.0, elapsedMs / 1000.0);
    LOG.info("DELETE complete: {} IDs ({} rows) in {} ms via {} SQL statements  ({} rows/sec)",
        deleteCount, expectedDeleted, elapsedMs, internalChunks,
        String.format("%.0f", deleteThroughput));

    long remainingCount = dao.count();
    LOG.info("Rows remaining after delete: {} (expected {})",
        remainingCount, expectedRemaining);

    assertEquals(expectedRemaining, remainingCount,
        "After deleting " + deleteCount + " container IDs, "
            + expectedRemaining + " rows should remain");

    assertTrue(elapsedMs <= TimeUnit.SECONDS.toMillis(MAX_DELETE_SECONDS),
        String.format("DELETE took %d ms, exceeded %d s threshold",
            elapsedMs, MAX_DELETE_SECONDS));
  }

  // -----------------------------------------------------------------------
  // Test 10 — Re-read counts after full delete
  // -----------------------------------------------------------------------

  /**
   * After full delete, verifies that each state has 0 records.
   */
  @Test
  @Order(10)
  public void testCountByStateAfterFullDelete() {
    int expectedPerState = 0;
    LOG.info("--- Test 10: COUNT by state after full delete (expected {} each) ---",
        expectedPerState);

    DSLContext dsl = schemaDefinition.getDSLContext();

    for (UnHealthyContainerStates state : TESTED_STATES) {
      long start = System.nanoTime();
      int stateCount = dsl
          .select(count())
          .from(UNHEALTHY_CONTAINERS)
          .where(UNHEALTHY_CONTAINERS.CONTAINER_STATE.eq(state.toString()))
          .fetchOne(0, int.class);
      long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

      LOG.info("  COUNT({}) = {} rows in {} ms", state, stateCount, elapsedMs);

      assertEquals(expectedPerState, stateCount,
          "After full delete, state " + state
              + " should have exactly " + expectedPerState + " records");
    }
  }

  // -----------------------------------------------------------------------
  // Helper — generate records for an inclusive container-ID range
  // -----------------------------------------------------------------------

  /**
   * Generates records for container IDs {@code [startId, endId]} across all
   * {@link #TESTED_STATES}.  Returning a range-bounded list rather than the
   * full 1 M rows keeps peak heap usage proportional to {@link #CONTAINERS_PER_TX}
   * rather than to {@link #TOTAL_RECORDS}.
   *
   * @param startId   first container ID (inclusive)
   * @param endId     last container ID (inclusive)
   * @param timestamp epoch millis to use as {@code in_state_since}
   * @return list of {@code (endId - startId + 1) × STATE_COUNT} records
   */
  private List<ContainerHealthSchemaManager.UnhealthyContainerRecord> generateRecordsForRange(
      int startId, int endId, long timestamp) {
    int size = (endId - startId + 1) * STATE_COUNT;
    List<UnhealthyContainerRecord> records = new ArrayList<>(size);

    for (int containerId = startId; containerId <= endId; containerId++) {
      for (UnHealthyContainerStates state : TESTED_STATES) {
        int expectedReplicas;
        int actualReplicas;
        String reason;

        switch (state) {
        case UNDER_REPLICATED:
          expectedReplicas = 3;
          actualReplicas = 2;
          reason = "Insufficient replicas";
          break;
        case MISSING:
          expectedReplicas = 3;
          actualReplicas = 0;
          reason = "No replicas available";
          break;
        case OVER_REPLICATED:
          expectedReplicas = 3;
          actualReplicas = 4;
          reason = "Excess replicas";
          break;
        case MIS_REPLICATED:
          expectedReplicas = 3;
          actualReplicas = 3;
          reason = "Placement policy violated";
          break;
        case EMPTY_MISSING:
          expectedReplicas = 1;
          actualReplicas = 0;
          reason = "Container has no replicas and no keys";
          break;
        default:
          expectedReplicas = 3;
          actualReplicas = 0;
          reason = "Unknown state";
        }

        records.add(new ContainerHealthSchemaManager.UnhealthyContainerRecord(
            containerId,
            state.toString(),
            timestamp,
            expectedReplicas,
            actualReplicas,
            expectedReplicas - actualReplicas,
            reason));
      }
    }
    return records;
  }
}
