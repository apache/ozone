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

package org.apache.hadoop.ozone.om;

import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.ozone.om.exceptions.OMException;

/**
 * Interface to manage Ozone snapshot DB local checkpoint metadata properties.
 * Those properties are per-OM, e.g. isSSTFiltered flag, which differs from the ones stored in SnapshotInfo proto.
 */
public interface OmSnapshotLocalProperty extends AutoCloseable {

  /**
   * Sets a property for a snapshot.
   *
   * @param key          Property key
   * @param value        Property value
   * @throws IOException if an I/O error occurs
   * @throws OMException if snapshot doesn't exist or operation fails
   */
  void setProperty(String key, String value) throws IOException;

  /**
   * Gets a property value for a snapshot.
   *
   * @param key          Property key
   * @return Property value or null if not found
   * @throws IOException if an I/O error occurs
   * @throws OMException if snapshot doesn't exist or operation fails
   */
  String getProperty(String key) throws IOException;

  /**
   * Gets all properties for a snapshot.
   *
   * @return Map of property key-value pairs
   * @throws IOException if an I/O error occurs
   * @throws OMException if snapshot doesn't exist or operation fails
   */
  Map<String, String> getProperties() throws IOException;

  /**
   * Checks if a property exists for a snapshot.
   *
   * @param key          Property key
   * @return true if the property exists, false otherwise
   * @throws IOException if an I/O error occurs
   * @throws OMException if snapshot doesn't exist or operation fails
   */
  boolean hasProperty(String key) throws IOException;

  /**
   * Removes a property from a snapshot.
   *
   * @param key          Property key
   * @throws IOException if an I/O error occurs
   * @throws OMException if snapshot doesn't exist or operation fails
   */
  void removeProperty(String key) throws IOException;
}
