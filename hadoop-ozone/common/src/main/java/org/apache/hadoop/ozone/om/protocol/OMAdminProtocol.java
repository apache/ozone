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

package org.apache.hadoop.ozone.om.protocol;

import java.io.Closeable;
import java.io.IOException;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.helpers.OMNodeDetails;
import org.apache.hadoop.security.KerberosInfo;

/**
 * Protocol for performing admin operations such as getting OM metadata.
 */
@KerberosInfo(
    serverPrincipal = OMConfigKeys.OZONE_OM_KERBEROS_PRINCIPAL_KEY)
public interface OMAdminProtocol extends Closeable {

  /**
   * Get the OM configuration.
   */
  OMConfiguration getOMConfiguration() throws IOException;

  /**
   * Remove OM from HA ring.
   */
  void decommission(OMNodeDetails removeOMNode) throws IOException;

  /**
   * Requests compaction of a column family of om.db.
   * @param columnFamily
   */
  void compactOMDB(String columnFamily) throws IOException;

  /**
   * Triggers the Snapshot Defragmentation Service to run immediately.
   * @param noWait if true, return immediately without waiting for completion
   * @return true if defragmentation completed successfully (when noWait is false),
   *         or if the task was triggered successfully (when noWait is true)
   */
  boolean triggerSnapshotDefrag(boolean noWait) throws IOException;
}
