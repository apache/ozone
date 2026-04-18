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
   * Total container count drift threshold above which the periodic incremental
   * sync escalates to a full SCM DB snapshot.
   *
   * <p>When {@code |(SCM_total_containers - SCM_open_containers) -
   * (Recon_total_containers - Recon_open_containers)|} exceeds this value the
   * targeted 4-pass sync becomes expensive (many batched RPC rounds) and a
   * full checkpoint replacement is cheaper and more reliable. The comparison
   * intentionally excludes OPEN containers because missing OPEN containers are
   * short-lived and can be repaired incrementally without replacing the full
   * SCM DB. For drift at or below this value the incremental sync corrects the
   * gap without replacing the entire database.
   *
   * <p>Note: a full snapshot is also scheduled unconditionally every 24h
   * (configurable via {@code ozone.recon.scm.snapshot.task.interval.delay})
   * as a structural safety net, independent of this threshold.
   *
   * <p>Default: 10,000. In large clusters (millions of containers) operators
   * may raise this further since the targeted sync handles per-state
   * corrections efficiently even at higher drift levels.
   */
  public static final String OZONE_RECON_SCM_CONTAINER_THRESHOLD =
      "ozone.recon.scm.container.threshold";
  public static final int OZONE_RECON_SCM_CONTAINER_THRESHOLD_DEFAULT = 10_000;

  public static final String OZONE_RECON_SCM_SNAPSHOT_ENABLED =
      "ozone.recon.scm.snapshot.enabled";
  public static final boolean OZONE_RECON_SCM_SNAPSHOT_ENABLED_DEFAULT = true;

  public static final String OZONE_RECON_SCM_CONNECTION_TIMEOUT =
      "ozone.recon.scm.connection.timeout";
  public static final String OZONE_RECON_SCM_CONNECTION_TIMEOUT_DEFAULT = "5s";

  public static final String OZONE_RECON_SCM_CONNECTION_REQUEST_TIMEOUT =
      "ozone.recon.scm.connection.request.timeout";
  public static final String
      OZONE_RECON_SCM_CONNECTION_REQUEST_TIMEOUT_DEFAULT = "5s";

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

  public static final String OZONE_RECON_SCM_SNAPSHOT_TASK_INTERVAL_DELAY =
      "ozone.recon.scm.snapshot.task.interval.delay";

  public static final String OZONE_RECON_SCM_SNAPSHOT_TASK_INTERVAL_DEFAULT
      = "24h";

  public static final String OZONE_RECON_SCM_SNAPSHOT_TASK_INITIAL_DELAY =
      "ozone.recon.scm.snapshot.task.initial.delay";

  public static final String
      OZONE_RECON_SCM_SNAPSHOT_TASK_INITIAL_DELAY_DEFAULT = "1m";

  /**
   * How often the incremental (targeted) SCM container sync runs.
   *
   * <p>Each cycle calls {@code decideSyncAction()} — two lightweight count
   * RPCs to SCM — and then either runs the 4-pass incremental sync or takes
   * no action. A full snapshot is still gated by
   * {@code ozone.recon.scm.snapshot.task.interval.delay} (default 24h).
   *
   * <p>Default: 1h. Set to a shorter value in environments where container
   * state discrepancies need to be detected and corrected faster.
   */
  public static final String OZONE_RECON_SCM_CONTAINER_SYNC_TASK_INTERVAL_DELAY =
      "ozone.recon.scm.container.sync.task.interval.delay";

  public static final String OZONE_RECON_SCM_CONTAINER_SYNC_TASK_INTERVAL_DEFAULT
      = "1h";

  /**
   * Initial delay before the first incremental SCM container sync run.
   *
   * <p>Default: 2m (slightly later than the snapshot initial delay of 1m,
   * so the snapshot has time to initialize the SCM DB before the first
   * incremental sync attempts to read it).
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
   * Maximum number of CLOSED/QUASI_CLOSED containers to check against SCM per
   * Pass 4 (DELETED retirement) sync cycle. Limiting the batch size prevents
   * excessive SCM RPC load during a single sync run; containers not checked in
   * one cycle are deferred to the next.
   *
   * <p>Default: 500 containers per sync cycle.
   */
  public static final String OZONE_RECON_SCM_DELETED_CONTAINER_CHECK_BATCH_SIZE =
      "ozone.recon.scm.deleted.container.check.batch.size";
  public static final int OZONE_RECON_SCM_DELETED_CONTAINER_CHECK_BATCH_SIZE_DEFAULT = 500;

  /**
   * Per-state drift threshold used by the tiered sync decision when the total
   * container count in SCM and Recon is equal.
   *
   * <p>Equal totals can still hide lifecycle state drift: a container that
   * advanced from OPEN → QUASI_CLOSED → CLOSED in SCM is counted in both SCM
   * and Recon's total, but Recon may still record it in the old state.
   * The following per-state comparisons are evaluated:
   *
   * <ul>
   *   <li><b>OPEN</b>: catches containers stuck OPEN in Recon after SCM has
   *       already moved them to CLOSING, QUASI_CLOSED, or CLOSED.</li>
   *   <li><b>QUASI_CLOSED</b>: catches containers stuck QUASI_CLOSED in Recon
   *       after SCM has already moved them to CLOSED or beyond.  This case is
   *       invisible to the OPEN check alone.</li>
   * </ul>
   *
   * <p>If the drift in <em>any</em> of the checked states exceeds this
   * threshold a targeted sync is triggered.  A full snapshot is deliberately
   * NOT triggered for per-state drift because the targeted sync's per-state
   * passes already correct these conditions efficiently without replacing the
   * entire database.
   *
   * <p>Default: 5.
   */
  public static final String OZONE_RECON_SCM_PER_STATE_DRIFT_THRESHOLD =
      "ozone.recon.scm.per.state.drift.threshold";
  public static final int OZONE_RECON_SCM_PER_STATE_DRIFT_THRESHOLD_DEFAULT = 5;

  /**
   * Private constructor for utility class.
   */
  private ReconServerConfigKeys() {
  }
}
