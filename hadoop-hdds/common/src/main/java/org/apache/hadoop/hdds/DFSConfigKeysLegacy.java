/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds;

/**
 * Legacy HDFS keys used by ozone.
 *
 * THey are the HDFS specific config keys. It would be better to use ozone
 * specific explicit configuration keys with ozone to make it more visible.
 */
@Deprecated
public final class DFSConfigKeysLegacy {

  private DFSConfigKeysLegacy() {
  }

  public static final String DFS_DATANODE_DNS_INTERFACE_KEY =
      "dfs.datanode.dns.interface";
  public static final String DFS_DATANODE_DNS_NAMESERVER_KEY =
      "dfs.datanode.dns.nameserver";

  public static final String DFS_DATANODE_HOST_NAME_KEY =
      "dfs.datanode.hostname";

  public static final String DFS_DATANODE_DATA_DIR_KEY =
      "dfs.datanode.data.dir";

  public static final String DFS_DATANODE_USE_DN_HOSTNAME =
      "dfs.datanode.use.datanode.hostname";

  public static final boolean DFS_DATANODE_USE_DN_HOSTNAME_DEFAULT = false;

  public static final String DFS_XFRAME_OPTION_ENABLED = "dfs.xframe.enabled";

  public static final boolean DFS_XFRAME_OPTION_ENABLED_DEFAULT = true;

  public static final String DFS_XFRAME_OPTION_VALUE = "dfs.xframe.value";

  public static final String DFS_XFRAME_OPTION_VALUE_DEFAULT = "SAMEORIGIN";

  public static final String DFS_METRICS_SESSION_ID_KEY =
      "dfs.metrics.session-id";

  public static final String NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY =
      "net.topology.node.switch.mapping.impl";

  public static final String DFS_CLIENT_HTTPS_KEYSTORE_RESOURCE_KEY =
      "dfs.client.https.keystore.resource";

  public static final String DFS_SERVER_HTTPS_KEYSTORE_RESOURCE_KEY =
      "dfs.https.server.keystore.resource";

  public static final String DFS_HTTP_POLICY_KEY = "dfs.http.policy";

  public static final String DFS_DATANODE_KERBEROS_PRINCIPAL_KEY =
      "dfs.datanode.kerberos.principal";

  public static final String DFS_DATANODE_KEYTAB_FILE_KEY =
      "dfs.datanode.keytab.file";

  public static final String DFS_DATANODE_DISK_CHECK_MIN_GAP_KEY =
      "dfs.datanode.disk.check.min.gap";

  public static final String DFS_DATANODE_DISK_CHECK_MIN_GAP_DEFAULT =
      "15m";

  public static final String DFS_DATANODE_DISK_CHECK_TIMEOUT_KEY =
      "dfs.datanode.disk.check.timeout";

  public static final String DFS_DATANODE_DISK_CHECK_TIMEOUT_DEFAULT =
      "10m";

  public static final String DFS_DATANODE_FAILED_VOLUMES_TOLERATED_KEY =
      "dfs.datanode.failed.volumes.tolerated";

  public static final int DFS_DATANODE_FAILED_VOLUMES_TOLERATED_DEFAULT = 0;

  public static final String DFS_METRICS_PERCENTILES_INTERVALS_KEY =
      "dfs.metrics.percentiles.intervals";

  public static final String DFS_WEB_AUTHENTICATION_KERBEROS_KEYTAB_KEY =
      "dfs.web.authentication.kerberos.keytab";

}


