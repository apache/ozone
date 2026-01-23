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

package org.apache.hadoop.hdds.scm.ha;

import com.google.protobuf.ByteString;
import java.io.IOException;
import org.apache.hadoop.hdds.scm.metadata.Replicate;
import org.apache.hadoop.hdds.utils.db.Table;

/**
 * This interface defines an API for saving and reading configurations of a
 * {@link StatefulService}.
 */
public interface StatefulServiceStateManager {

  /**
   * Persists the specified configurations bytes to RocksDB and replicates
   * this operation through RATIS. The specified serviceName maps to the
   * persisted bytes.
   * @param serviceName name of the {@link StatefulService}, obtained
   *                    through {@link SCMService#getServiceName()}
   * @param bytes configuration to persist as a ByteString
   * @throws IOException on failure to persist configuration
   */
  @Replicate
  void saveConfiguration(String serviceName, ByteString bytes)
      throws IOException;

  /**
   * Reads the persisted configuration mapped to the specified serviceName.
   * @param serviceName name of the {@link StatefulService}, obtained through
   * {@link SCMService#getServiceName()}
   * @return configuration as a ByteString
   * @throws IOException on failure
   */
  ByteString readConfiguration(String serviceName) throws IOException;

  /**
   * Deletes the persisted configuration mapped to the specified serviceName.
   * @param serviceName name of the {@link StatefulService}, obtained through
   * {@link SCMService#getServiceName()}
   * @throws IOException on failure
   */
  @Replicate
  void deleteConfiguration(String serviceName) throws IOException;

  /**
   * Sets the updated reference to the table when reloading SCM state.
   * @param statefulServiceConfig table from
   * {@link org.apache.hadoop.hdds.scm.metadata.SCMMetadataStore}
   */
  void reinitialize(Table<String, ByteString> statefulServiceConfig);
}
