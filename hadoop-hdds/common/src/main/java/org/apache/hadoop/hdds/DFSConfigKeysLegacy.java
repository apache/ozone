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
      "hdds.datanode.dns.interface";
  public static final String DFS_DATANODE_DNS_NAMESERVER_KEY =
      "hdds.datanode.dns.nameserver";

  public static final String DFS_DATANODE_HOST_NAME_KEY =
      "hdds.datanode.hostname";

  public static final String DFS_DATANODE_DATA_DIR_KEY =
      "hdds.datanode.data.dir";

  public static final String DFS_DATANODE_USE_DN_HOSTNAME =
      "hdds.datanode.use.datanode.hostname";

  public static final boolean DFS_DATANODE_USE_DN_HOSTNAME_DEFAULT = false;

  public static final String DFS_XFRAME_OPTION_ENABLED = "hdds.xframe.enabled";

  public static final boolean DFS_XFRAME_OPTION_ENABLED_DEFAULT = true;

  public static final String DFS_XFRAME_OPTION_VALUE = "hdds.xframe.value";

  public static final String DFS_XFRAME_OPTION_VALUE_DEFAULT = "SAMEORIGIN";

  public static final String DFS_METRICS_SESSION_ID_KEY =
      "hdds.metrics.session-id";

  public static final String NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY =
      "net.topology.node.switch.mapping.impl";

  public static final String DFS_DATANODE_KERBEROS_PRINCIPAL_KEY =
      "hdds.datanode.kerberos.principal";

  public static final String DFS_DATANODE_KERBEROS_KEYTAB_FILE_KEY =
      "hdds.datanode.kerberos.keytab.file";

  public static final String DFS_METRICS_PERCENTILES_INTERVALS_KEY =
      "hdds.metrics.percentiles.intervals";

}


