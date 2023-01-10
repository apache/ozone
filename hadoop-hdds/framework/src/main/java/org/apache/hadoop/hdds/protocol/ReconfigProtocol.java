/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.ReconfigurationTaskStatus;
import org.apache.hadoop.io.retry.Idempotent;

import java.io.IOException;
import java.util.List;

/**********************************************************************
 * ReconfigProtocol is used by ozone admin to reload configuration.
 **********************************************************************/
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface ReconfigProtocol {

  long VERSIONID = 1L;

  /**
   * Asynchronously reload configuration on disk and apply changes.
   */
  @Idempotent
  void startReconfig() throws IOException;

  /**
   * Get the status of the previously issued reconfig task.
   * @see ReconfigurationTaskStatus
   */
  @Idempotent
  ReconfigurationTaskStatus getReconfigStatus() throws IOException;

  /**
   * Get a list of allowed properties for reconfiguration.
   */
  @Idempotent
  List<String> listReconfigProperties() throws IOException;
}
