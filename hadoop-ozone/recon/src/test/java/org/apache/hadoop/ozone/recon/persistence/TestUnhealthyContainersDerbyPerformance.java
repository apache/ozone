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
import org.apache.hadoop.ozone.recon.persistence.ContainerHealthSchemaManagerV2.UnhealthyContainerRecordV2;
import org.apache.hadoop.ozone.recon.persistence.ContainerHealthSchemaManagerV2.UnhealthyContainersSummaryV2;
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
 *       container IDs (100 K × 5 states = 500 K rows) via a single
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
 *   <li>Uses {@code @TestInstance(PER_CLASS)} so the 1 M-row dataset is
 *       inserted exactly once in {@code @BeforeAll} and shared across all
 *       {@code @Test} methods in the class.</li>
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
   * {@link ContainerHealthSchemaManagerV2#batchDeleteSCMStatesForContainers}
   * call in the delete test.
   *
   * <p>{@code batchDeleteSCMStatesForContainers} now handles internal
   * chunking at {@link ContainerHealthSchemaManagerV2#MAX_DELETE_CHUNK_SIZE}
   * ({@value ContainerHealthSchemaManagerV2#MAX_DELETE_CHUNK_SIZE} IDs per
   * SQL statement) to stay within Derby's 64 KB generated-bytecode limit
   * (ERROR XBCM4).  This test-level constant controls how many IDs are
   * accumulated before each call and should match that limit so the test
   * exercises exactly one SQL DELETE per call.</p>
   */
  private static final int DELETE_CHUNK_SIZE = 1_000;   // matches MAX_DELETE_CHUNK_SIZE

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

  /** Maximum acceptable time to batch-delete 500 K rows. */
  private static final long MAX_DELETE_SECONDS = 60;

  // -----------------------------------------------------------------------
  // Infrastructure (shared for the life of this test class)
  // -----------------------------------------------------------------------

  private ContainerHealthSchemaManagerV2 schemaManager;
  private UnhealthyContainersDao dao;
  private ContainerSchemaDefinition schemaDefinition;

  // -----------------------------------------------------------------------
  // One-time setup: create Derby schema + insert 1 M records
  // -----------------------------------------------------------------------

  /**
   * Initialises the embedded Derby database, creates the Recon schema, and
   * inserts {@value #TOTAL_RECORDS} records.  This runs exactly once for the
   * entire test class.
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
  public void setUpDatabaseAndInsertData(@TempDir Path tempDir) throws Exception {
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
    schemaManager = new ContainerHealthSchemaManagerV2(schemaDefinition, dao);

    // ----- Insert 1 M records in small per-transaction chunks -----
    //
    // Why chunked?  insertUnhealthyContainerRecords wraps its entire input in
    // a single Derby transaction.  Passing all 1 M records at once forces Derby
    // to buffer the full WAL before committing, which exhausts its log and
    // causes the call to hang.  Committing every CONTAINERS_PER_TX containers
    // (= 10 K rows) keeps each transaction small and lets Derby flush the log.
    int txCount = (int) Math.ceil((double) CONTAINER_ID_RANGE / CONTAINERS_PER_TX);
    LOG.info("Starting bulk INSERT: {} records  ({} containers/tx, {} transactions)",
        TOTAL_RECORDS, CONTAINERS_PER_TX, txCount);

    long now = System.currentTimeMillis();
    long insertStart = System.nanoTime();

    for (int startId = 1; startId <= CONTAINER_ID_RANGE; startId += CONTAINERS_PER_TX) {
      int endId = Math.min(startId + CONTAINERS_PER_TX - 1, CONTAINER_ID_RANGE);
      List<UnhealthyContainerRecordV2> chunk = generateRecordsForRange(startId, endId, now);
      schemaManager.insertUnhealthyContainerRecords(chunk);
    }

    long insertElapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - insertStart);
    double insertThroughput = (double) TOTAL_RECORDS / (insertElapsedMs / 1000.0);
    LOG.info("INSERT complete: {} records in {} ms  ({} rec/sec, {} tx)",
        TOTAL_RECORDS, insertElapsedMs, String.format("%.0f", insertThroughput), txCount);

    assertTrue(insertElapsedMs <= TimeUnit.SECONDS.toMillis(MAX_INSERT_SECONDS),
        String.format("INSERT took %d ms, exceeded %d s threshold",
            insertElapsedMs, MAX_INSERT_SECONDS));
  }

  // -----------------------------------------------------------------------
  // Test 1 — Verify the inserted row count
  // -----------------------------------------------------------------------

  /**
   * Verifies that all {@value #TOTAL_RECORDS} rows are present using a
   * COUNT(*) over the full table.  This is the baseline correctness check
   * for every subsequent read test.
   */
  @Test
  @Order(1)
  public void testTotalInsertedRecordCountIsOneMillion() {
    LOG.info("--- Test 1: Verify total row count = {} ---", TOTAL_RECORDS);

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
  @Order(2)
  public void testCountByStatePerformanceUsesIndex() {
    LOG.info("--- Test 2: COUNT(*) by state (index-covered, {} records each) ---",
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
   * Runs the {@link ContainerHealthSchemaManagerV2#getUnhealthyContainersSummary()}
   * GROUP-BY query over all 1 M rows, which represents a typical API request
   * to populate the Recon UI dashboard.
   *
   * <p>Expected result: {@value #STATE_COUNT} state groups, each with
   * {@value #CONTAINER_ID_RANGE} records.</p>
   */
  @Test
  @Order(3)
  public void testGroupBySummaryQueryPerformance() {
    LOG.info("--- Test 3: GROUP BY summary over {} rows ---", TOTAL_RECORDS);

    long start = System.nanoTime();
    List<UnhealthyContainersSummaryV2> summary =
        schemaManager.getUnhealthyContainersSummary();
    long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

    LOG.info("GROUP BY summary: {} state groups returned in {} ms",
        summary.size(), elapsedMs);
    summary.forEach(s ->
        LOG.info("  state={} count={}", s.getContainerState(), s.getCount()));

    assertEquals(STATE_COUNT, summary.size(),
        "Summary must contain one entry per tested state");

    for (UnhealthyContainersSummaryV2 entry : summary) {
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
  @Order(4)
  public void testPaginatedReadByStatePerformance() {
    UnHealthyContainerStates targetState = UnHealthyContainerStates.UNDER_REPLICATED;
    LOG.info("--- Test 4: Paginated read of {} ({} records, page size {}) ---",
        targetState, CONTAINER_ID_RANGE, READ_PAGE_SIZE);

    int totalRead = 0;
    int pageCount = 0;
    long minContainerId = 0;
    long lastContainerId = -1;
    boolean orderedCorrectly = true;

    long start = System.nanoTime();

    while (true) {
      List<UnhealthyContainerRecordV2> page =
          schemaManager.getUnhealthyContainers(
              targetState, minContainerId, 0, READ_PAGE_SIZE);

      if (page.isEmpty()) {
        break;
      }

      for (UnhealthyContainerRecordV2 rec : page) {
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
  @Order(5)
  public void testFullDatasetReadThroughputAllStates() {
    LOG.info("--- Test 5: Full {} M record read (all states, paged) ---",
        TOTAL_RECORDS / 1_000_000);

    long totalStart = System.nanoTime();
    Map<UnHealthyContainerStates, Integer> countPerState =
        new EnumMap<>(UnHealthyContainerStates.class);

    for (UnHealthyContainerStates state : TESTED_STATES) {
      long stateStart = System.nanoTime();
      int stateTotal = 0;
      long minId = 0;

      while (true) {
        List<UnhealthyContainerRecordV2> page =
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
  // Test 6 — Batch DELETE performance
  // -----------------------------------------------------------------------

  /**
   * Deletes records for the first half of container IDs (1 – 100,000) across
   * all five states by passing the complete 100 K ID list in one call to
   * {@link ContainerHealthSchemaManagerV2#batchDeleteSCMStatesForContainers}.
   *
   * <p>{@code batchDeleteSCMStatesForContainers} now handles internal
   * chunking at {@link ContainerHealthSchemaManagerV2#MAX_DELETE_CHUNK_SIZE}
   * IDs per SQL statement to stay within Derby's 64 KB generated-bytecode
   * limit (JVM ERROR XBCM4).  Passing 100 K IDs in a single call is safe
   * because the method partitions them internally into 100 statements of
   * 1,000 IDs each — matching Recon's real scan-cycle pattern for large
   * clusters.</p>
   *
   * <p>Expected outcome: 100 K × 5 states = 500 K rows deleted, 500 K remain.</p>
   *
   * <p><b>Note:</b> this test modifies the shared dataset, so it runs after
   * all read-only tests.</p>
   */
  @Test
  @Order(6)
  public void testBatchDeletePerformanceHalfTheContainers() {
    int deleteCount = CONTAINER_ID_RANGE / 2;           // 100 000 container IDs
    int expectedDeleted = deleteCount * STATE_COUNT;    // 500 000 rows
    int expectedRemaining = TOTAL_RECORDS - expectedDeleted;
    int internalChunks = (int) Math.ceil(
        (double) deleteCount / ContainerHealthSchemaManagerV2.MAX_DELETE_CHUNK_SIZE);

    LOG.info("--- Test 6: Batch DELETE — {} IDs × {} states = {} rows  "
            + "({} internal SQL statements of {} IDs) ---",
        deleteCount, STATE_COUNT, expectedDeleted,
        internalChunks, ContainerHealthSchemaManagerV2.MAX_DELETE_CHUNK_SIZE);

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
  // Test 7 — Re-read counts after partial delete
  // -----------------------------------------------------------------------

  /**
   * After the deletion in Test 6, verifies that each state has exactly
   * {@code CONTAINER_ID_RANGE / 2} records (100 K), confirming that the
   * index-covered COUNT query remains accurate after a large delete.
   */
  @Test
  @Order(7)
  public void testCountByStateAfterPartialDelete() {
    int expectedPerState = CONTAINER_ID_RANGE / 2;
    LOG.info("--- Test 7: COUNT by state after 50%% delete (expected {} each) ---",
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
          "After partial delete, state " + state
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
  private List<UnhealthyContainerRecordV2> generateRecordsForRange(
      int startId, int endId, long timestamp) {
    int size = (endId - startId + 1) * STATE_COUNT;
    List<UnhealthyContainerRecordV2> records = new ArrayList<>(size);

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

        records.add(new UnhealthyContainerRecordV2(
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
