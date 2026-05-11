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

package org.apache.hadoop.ozone.recon;

import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;

/**
 * This class contains constants for Recon configuration keys.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public final class  ReconServerConfigKeys {

  public static final String OZONE_RECON_HTTP_ENABLED_KEY =
      "ozone.recon.http.enabled";
  public static final String OZONE_RECON_HTTP_BIND_HOST_KEY =
      "ozone.recon.http-bind-host";
  public static final String OZONE_RECON_HTTPS_BIND_HOST_KEY =
      "ozone.recon.https-bind-host";
  public static final String OZONE_RECON_HTTP_KEYTAB_FILE =
      "ozone.recon.http.auth.kerberos.keytab";
  public static final String OZONE_RECON_HTTP_BIND_HOST_DEFAULT =
      "0.0.0.0";
  public static final int OZONE_RECON_HTTP_BIND_PORT_DEFAULT = 9888;
  public static final int OZONE_RECON_HTTPS_BIND_PORT_DEFAULT = 9889;
  public static final String OZONE_RECON_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL =
      "ozone.recon.http.auth.kerberos.principal";

  public static final String OZONE_RECON_DB_DIR = "ozone.recon.db.dir";

  public static final String OZONE_RECON_OM_SNAPSHOT_DB_DIR =
      "ozone.recon.om.db.dir";

  public static final String OZONE_RECON_SCM_DB_DIR =
      "ozone.recon.scm.db.dirs";

  public static final String RECON_STORAGE_DIR = "recon";

  public static final String OZONE_RECON_OM_SOCKET_TIMEOUT =
      "ozone.recon.om.socket.timeout";
  public static final String OZONE_RECON_OM_SOCKET_TIMEOUT_DEFAULT = "5s";
  @Deprecated
  public static final String RECON_OM_SOCKET_TIMEOUT =
      "recon.om.socket.timeout";

  public static final String OZONE_RECON_OM_CONNECTION_TIMEOUT =
      "ozone.recon.om.connection.timeout";
  public static final String OZONE_RECON_OM_CONNECTION_TIMEOUT_DEFAULT = "5s";
  @Deprecated
  public static final String RECON_OM_CONNECTION_TIMEOUT =
      "recon.om.connection.timeout";

  public static final String OZONE_RECON_OM_CONNECTION_REQUEST_TIMEOUT =
      "ozone.recon.om.connection.request.timeout";
  public static final String OZONE_RECON_OM_CONNECTION_REQUEST_TIMEOUT_DEFAULT =
      "5s";
  @Deprecated
  public static final String RECON_OM_CONNECTION_REQUEST_TIMEOUT =
      "recon.om.connection.request.timeout";

  public static final String OZONE_RECON_OM_SNAPSHOT_TASK_INITIAL_DELAY =
      "ozone.recon.om.snapshot.task.initial.delay";
  public static final String
      OZONE_RECON_OM_SNAPSHOT_TASK_INITIAL_DELAY_DEFAULT = "1m";
  @Deprecated
  public static final String RECON_OM_SNAPSHOT_TASK_INITIAL_DELAY =
      "recon.om.snapshot.task.initial.delay";

  public static final String OZONE_RECON_OM_SNAPSHOT_TASK_INTERVAL_DELAY =
      "ozone.recon.om.snapshot.task.interval.delay";
  public static final String OZONE_RECON_OM_SNAPSHOT_TASK_INTERVAL_DEFAULT
      = "5s";
  @Deprecated
  public static final String RECON_OM_SNAPSHOT_TASK_INTERVAL_DELAY =
      "recon.om.snapshot.task.interval.delay";

  public static final String OZONE_RECON_OM_SNAPSHOT_TASK_FLUSH_PARAM =
      "ozone.recon.om.snapshot.task.flush.param";
  @Deprecated
  public static final String RECON_OM_SNAPSHOT_TASK_FLUSH_PARAM =
      "recon.om.snapshot.task.flush.param";

  public static final String RECON_OM_DELTA_UPDATE_LIMIT =
      "recon.om.delta.update.limit";
  public static final long RECON_OM_DELTA_UPDATE_LIMIT_DEFAULT = 50000;

  public static final String RECON_OM_DELTA_UPDATE_LAG_THRESHOLD =
      "recon.om.delta.update.lag.threshold";
  public static final long RECON_OM_DELTA_UPDATE_LAG_THRESHOLD_DEFAULT = 0;

  public static final String OZONE_RECON_TASK_THREAD_COUNT_KEY =
      "ozone.recon.task.thread.count";
  public static final int OZONE_RECON_TASK_THREAD_COUNT_DEFAULT = 8;

  public static final String OZONE_RECON_OM_EVENT_BUFFER_CAPACITY =
      "ozone.recon.om.event.buffer.capacity";
  public static final int OZONE_RECON_OM_EVENT_BUFFER_CAPACITY_DEFAULT = 20000;

  public static final String OZONE_RECON_HTTP_AUTH_CONFIG_PREFIX =
      "ozone.recon.http.auth.";

  public static final String OZONE_RECON_HTTP_AUTH_TYPE =
      OZONE_RECON_HTTP_AUTH_CONFIG_PREFIX + "type";

  public static final String OZONE_RECON_METRICS_HTTP_CONNECTION_TIMEOUT =
      "ozone.recon.metrics.http.connection.timeout";

  public static final String
      OZONE_RECON_METRICS_HTTP_CONNECTION_TIMEOUT_DEFAULT =
      "30s";

  public static final String
      OZONE_RECON_METRICS_HTTP_CONNECTION_REQUEST_TIMEOUT =
      "ozone.recon.metrics.http.connection.request.timeout";

  public static final String
      OZONE_RECON_METRICS_HTTP_CONNECTION_REQUEST_TIMEOUT_DEFAULT = "60s";

  /**
   * Non-OPEN container count drift threshold above which the periodic
   * incremental sync records a large-drift event.
   *
   * <p>When {@code |(SCM_total_containers - SCM_open_containers) -
   * (Recon_total_containers - Recon_open_containers)|} exceeds this value the
   * targeted 4-pass sync becomes expensive (many batched RPC rounds). The
   * periodic scheduler records the condition through logs and metrics rather
   * than automatically replacing the SCM DB snapshot. The comparison
   * intentionally excludes OPEN containers because missing OPEN containers are
   * short-lived and can be repaired incrementally.
   *
   * <p>Default: 1,000,000. In large clusters (millions of containers) operators
   * may raise this further since the targeted sync handles per-state
   * corrections efficiently even at higher drift levels.
   */
  public static final String OZONE_RECON_SCM_CONTAINER_THRESHOLD =
      "ozone.recon.scm.container.threshold";
  public static final int OZONE_RECON_SCM_CONTAINER_THRESHOLD_DEFAULT = 1_000_000;

  public static final String OZONE_RECON_SCM_SNAPSHOT_ENABLED =
      "ozone.recon.scm.snapshot.enabled";
  public static final boolean OZONE_RECON_SCM_SNAPSHOT_ENABLED_DEFAULT = true;

  public static final String OZONE_RECON_NSSUMMARY_FLUSH_TO_DB_MAX_THRESHOLD =
      "ozone.recon.nssummary.flush.db.max.threshold";

  public static final long
      OZONE_RECON_NSSUMMARY_FLUSH_TO_DB_MAX_THRESHOLD_DEFAULT = 150 * 1000L * 2;

  public static final String
      OZONE_RECON_CONTAINER_KEY_FLUSH_TO_DB_MAX_THRESHOLD =
      "ozone.recon.containerkey.flush.db.max.threshold";

  public static final long
      OZONE_RECON_CONTAINER_KEY_FLUSH_TO_DB_MAX_THRESHOLD_DEFAULT = 150 * 1000L;

  public static final String
      OZONE_RECON_FILESIZECOUNT_FLUSH_TO_DB_MAX_THRESHOLD =
      "ozone.recon.filesizecount.flush.db.max.threshold";

  public static final long
      OZONE_RECON_FILESIZECOUNT_FLUSH_TO_DB_MAX_THRESHOLD_DEFAULT = 200 * 1000L;

  public static final String
      OZONE_RECON_TASK_REPROCESS_MAX_ITERATORS = "ozone.recon.task.reprocess.max.iterators";

  public static final int OZONE_RECON_TASK_REPROCESS_MAX_ITERATORS_DEFAULT = 5;

  public static final String
      OZONE_RECON_TASK_REPROCESS_MAX_WORKERS = "ozone.recon.task.reprocess.max.workers";

  public static final int OZONE_RECON_TASK_REPROCESS_MAX_WORKERS_DEFAULT = 20;

  public static final String
      OZONE_RECON_TASK_REPROCESS_MAX_KEYS_IN_MEMORY = "ozone.recon.task.reprocess.max.keys.in.memory";

  public static final int OZONE_RECON_TASK_REPROCESS_MAX_KEYS_IN_MEMORY_DEFAULT = 2000;

  /**
   * How often the incremental (targeted) SCM container sync runs.
   *
   * <p>Each cycle calls {@code decideSyncAction()} — two lightweight count
   * RPCs to SCM — and then either runs the 4-pass incremental sync or takes
   * no action. This periodic task does not download a full SCM DB snapshot
   * automatically.
   *
   * <p>Default:
   * {@link #OZONE_RECON_SCM_CONTAINER_SYNC_TASK_INTERVAL_DEFAULT}. Set to a
   * shorter value in environments where container state discrepancies need to
   * be detected and corrected faster.
   */
  public static final String OZONE_RECON_SCM_CONTAINER_SYNC_TASK_INTERVAL_DELAY =
      "ozone.recon.scm.container.sync.task.interval.delay";

  public static final String OZONE_RECON_SCM_CONTAINER_SYNC_TASK_INTERVAL_DEFAULT
      = "6h";

  /**
   * Initial delay before the first incremental SCM container sync run.
   *
   * <p>Default: 2m, giving Recon startup enough time to initialize the SCM DB
   * before the first incremental sync attempts to read it.
   */
  public static final String OZONE_RECON_SCM_CONTAINER_SYNC_TASK_INITIAL_DELAY =
      "ozone.recon.scm.container.sync.task.initial.delay";

  public static final String
      OZONE_RECON_SCM_CONTAINER_SYNC_TASK_INITIAL_DELAY_DEFAULT = "2m";

  public static final String OZONE_RECON_SCM_CLIENT_RPC_TIME_OUT_KEY =
      "ozone.recon.scmclient.rpc.timeout";

  public static final String OZONE_RECON_SCM_CLIENT_RPC_TIME_OUT_DEFAULT = "1m";

  public static final String OZONE_RECON_SCM_CLIENT_MAX_RETRY_TIMEOUT_KEY =
      "ozone.recon.scmclient.max.retry.timeout";

  public static final String OZONE_RECON_SCM_CLIENT_MAX_RETRY_TIMEOUT_DEFAULT =
      "6s";

  public static final String OZONE_RECON_SCM_CLIENT_FAILOVER_MAX_RETRY_KEY =
      "ozone.recon.scmclient.failover.max.retry";

  public static final int
      OZONE_RECON_SCM_CLIENT_FAILOVER_MAX_RETRY_DEFAULT = 3;

  public static final String OZONE_RECON_DN_METRICS_COLLECTION_MINIMUM_API_DELAY =
      "ozone.recon.dn.metrics.collection.minimum.api.delay";
  public static final String OZONE_RECON_DN_METRICS_COLLECTION_MINIMUM_API_DELAY_DEFAULT = "30s";

  public static final String OZONE_RECON_DN_METRICS_COLLECTION_TIMEOUT =
      "ozone.recon.dn.metrics.collection.timeout";
  public static final String OZONE_RECON_DN_METRICS_COLLECTION_TIMEOUT_DEFAULT = "10m";

  /**
   * Application-level ceiling on the number of ContainerIDs fetched from SCM
   * per RPC call during container sync. The effective batch size is
   * {@code min(this value, ipc.maximum.data.length / 12, totalContainerCount)},
   * so raising this above the default is only meaningful if
   * {@code ipc.maximum.data.length} has also been raised from its default.
   *
   * <p><b>Recon wire cost</b>: each ContainerID is ~12 bytes on the wire, so
   * the default 1,000,000 produces ~12 MB per RPC.
   *
   * <p><b>Recon JVM heap</b>: each deserialized {@code ContainerID} object
   * occupies ~32 bytes, so the default batch requires ~32 MB of heap on Recon.
   * Reduce this value on memory-constrained Recon nodes.
   *
   * <p><b>SCM-side pressure</b>: on each RPC call SCM holds its container
   * state read lock (a fair {@link java.util.concurrent.locks.ReentrantReadWriteLock})
   * for the full duration of streaming N entries from its in-memory
   * {@link java.util.TreeMap} and collecting them into a response list.
   * Because the lock is fair, any concurrent write (container allocation,
   * state transition) queuing for the write lock will be blocked for the
   * entire batch duration — and new reads queue behind that waiting writer.
   * Larger batches therefore increase worst-case container-allocation latency
   * on SCM during sync. On write-heavy SCM nodes, prefer smaller batches with
   * more calls over fewer large batches.
   *
   * <p>Default: 1,000,000 (~12 MB wire, ~32 MB JVM heap per batch on Recon;
   * 4 calls for a 4 M-container cluster)
   */
  public static final String OZONE_RECON_SCM_CONTAINER_ID_BATCH_SIZE =
      "ozone.recon.scm.container.id.batch.size";
  public static final long OZONE_RECON_SCM_CONTAINER_ID_BATCH_SIZE_DEFAULT = 1_000_000;

  /**
   * Page size for Pass 4 (DELETED retirement) in each TARGETED_SYNC cycle.
   *
   * <p>Pass 4 paginates SCM's DELETED list using {@code getListOfContainerInfos},
   * which returns {@code ContainerInfo} objects (~86 bytes each on wire, no
   * pipeline or DatanodeDetails). The safe IPC upper bound at 128 MB default is
   * {@code 128 MB / 128 bytes = 1,048,576} containers per page.
   *
   * <p>At the default of 1,000,000 per page:
   * <ul>
   *   <li>Wire payload: 1M × 86 bytes ≈ 82 MB — within the 128 MB IPC limit.</li>
   *   <li>JVM heap per page: 1M × ~300 bytes ≈ 286 MB — processed one page at a
   *       time and GC'd before the next page is fetched.</li>
   *   <li>Even 1 billion DELETED containers require only ~1,000 page calls per
   *       sync cycle, each completing quickly.</li>
   * </ul>
   *
   * <p>The value is automatically capped at
   * {@code ipc.maximum.data.length / 128} (1,048,576 at the 128 MB default)
   * regardless of what is configured here.
   *
   * <p>Default: 1,000,000 containers per page.
   */
  public static final String OZONE_RECON_SCM_DELETED_CONTAINER_CHECK_BATCH_SIZE =
      "ozone.recon.scm.deleted.container.check.batch.size";
  public static final int OZONE_RECON_SCM_DELETED_CONTAINER_CHECK_BATCH_SIZE_DEFAULT = 1_000_000;

  /**
   * Per-state drift threshold used by the tiered sync decision for stable
   * lifecycle states.
   *
   * <p>Equal totals can still hide lifecycle state drift: a container that
   * advanced from OPEN → QUASI_CLOSED → CLOSED in SCM is counted in both SCM
   * and Recon's total, but Recon may still record it in the old state. OPEN
   * drift always triggers targeted sync when it is non-zero; this threshold is
   * applied to the following stable-state comparisons:
   *
   * <ul>
   *   <li><b>QUASI_CLOSED</b>: catches containers stuck QUASI_CLOSED in Recon
   *       after SCM has already moved them to CLOSED or beyond.  This case is
   *       invisible to the OPEN check alone.</li>
   *   <li><b>CLOSED</b>: catches repairable CLOSED count mismatch without using
   *       all-state total drift, which may include non-repairable DELETED-only
   *       differences.</li>
   * </ul>
   *
   * <p>If the drift in <em>any</em> of the checked states reaches this
   * threshold, a targeted sync is triggered. A full snapshot is deliberately
   * NOT triggered because the targeted sync's per-state passes correct these
   * conditions efficiently without replacing the entire database.
   *
   * <p>The check uses an inclusive {@code >=} comparison. A value of 1 means
   * any single wrong-state container triggers TARGETED_SYNC at the next
   * scheduled check.
   * This is the recommended default because:
   * <ul>
   *   <li>TARGETED_SYNC runs at most once per scheduled interval (default 6h)
   *       regardless of how many times drift is detected.</li>
   *   <li>All four sync passes are idempotent — running unnecessarily has no
   *       side effects and completes quickly in steady state.</li>
   *   <li>A per-state threshold of 5 tolerates up to 4 wrong-state containers
   *       permanently if total counts happen to match by coincidence, which
   *       compromises Recon's monitoring accuracy.</li>
   * </ul>
   *
   * <p>Default: 1.
   */
  public static final String OZONE_RECON_SCM_PER_STATE_DRIFT_THRESHOLD =
      "ozone.recon.scm.per.state.drift.threshold";
  public static final int OZONE_RECON_SCM_PER_STATE_DRIFT_THRESHOLD_DEFAULT = 1;

  /**
   * JDBC fetch size for CSV exports.
   * Default: 10,000 rows per fetch
   */
  public static final String OZONE_RECON_UNHEALTHY_CONTAINER_FETCH_SIZE =
      "ozone.recon.unhealthy.container.fetch.size";
  public static final int OZONE_RECON_UNHEALTHY_CONTAINER_FETCH_SIZE_DEFAULT = 10_000;

  /**
   * Max export jobs that can sit in the queue (waiting + executing) at once.
   * Submissions beyond this limit are rejected with HTTP 429.
   * Kept small because export is single-threaded and the unhealthy-container
   * states it can be invoked for are bounded (~5).
   * Default: 4
   */
  public static final String OZONE_RECON_EXPORT_MAX_JOBS_TOTAL =
      "ozone.recon.export.max.jobs.total";
  public static final int OZONE_RECON_EXPORT_MAX_JOBS_TOTAL_DEFAULT = 4;

  /**
   * Directory to store export CSV files.
   * Default: /tmp/recon/exports
   */
  public static final String OZONE_RECON_EXPORT_DIRECTORY =
      "ozone.recon.export.directory";

  // Default is resolved at runtime as {ozone.recon.db.dir}/exports.
  // Empty string signals ExportJobManager to compute the path dynamically.
  public static final String OZONE_RECON_EXPORT_DIRECTORY_DEFAULT = "";

  /**
   * Maximum number of times a completed export TAR file can be downloaded.
   * Prevents repeated downloads from filling up network bandwidth or being misused.
   * Default: 3
   */
  public static final String OZONE_RECON_EXPORT_MAX_DOWNLOADS =
      "ozone.recon.export.max.downloads";
  public static final int OZONE_RECON_EXPORT_MAX_DOWNLOADS_DEFAULT = 3;

  /**
   * Private constructor for utility class.
   */
  private ReconServerConfigKeys() {
  }
}
