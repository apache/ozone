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

  public static final String OZONE_RECON_SCM_CONTAINER_THRESHOLD =
      "ozone.recon.scm.container.threshold";
  public static final int OZONE_RECON_SCM_CONTAINER_THRESHOLD_DEFAULT = 100;

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
      OZONE_RECON_NSSUMMARY_FLUSH_TO_DB_MAX_THRESHOLD_DEFAULT = 150 * 1000L;

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
   * Private constructor for utility class.
   */
  private ReconServerConfigKeys() {
  }
}
