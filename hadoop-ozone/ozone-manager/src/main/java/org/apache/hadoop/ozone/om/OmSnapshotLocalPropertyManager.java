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

import org.apache.hadoop.ozone.om.exceptions.OMException;

import java.io.IOException;
import java.util.Map;

/**
 * Interface to manage Ozone snapshot DB local checkpoint metadata properties.
 * Those properties are per-OM, e.g. isSSTFiltered flag, which differs from the ones stored in SnapshotInfo proto.
 */
public interface OmSnapshotLocalPropertyManager {

  /**
   * Sets a property for a snapshot.
   *
   * @param yamlFilePath Path to the snapshot's YAML property file
   * @param key Property key
   * @param value Property value
   * @throws IOException if an I/O error occurs
   * @throws OMException if snapshot doesn't exist or operation fails
   */
  void setProperty(String yamlFilePath, String key, String value)
      throws IOException, OMException;

  /**
   * Gets a property value for a snapshot.
   *
   * @param yamlFilePath Path to the snapshot's YAML property file
   * @param key Property key
   * @return Property value or null if not found
   * @throws IOException if an I/O error occurs
   * @throws OMException if snapshot doesn't exist or operation fails
   */
  String getProperty(String yamlFilePath, String key)
      throws IOException, OMException;

  /**
   * Gets all properties for a snapshot.
   *
   * @param yamlFilePath Path to the snapshot's YAML property file
   * @return Map of property key-value pairs
   * @throws IOException if an I/O error occurs
   * @throws OMException if snapshot doesn't exist or operation fails
   */
  Map<String, String> getProperties(String yamlFilePath)
      throws IOException, OMException;

  /**
   * Checks if a property exists for a snapshot.
   *
   * @param yamlFilePath Path to the snapshot's YAML property file
   * @param key Property key
   * @return true if the property exists, false otherwise
   * @throws IOException if an I/O error occurs
   * @throws OMException if snapshot doesn't exist or operation fails
   */
  boolean hasProperty(String yamlFilePath, String key)
      throws IOException, OMException;

  /**
   * Removes a property from a snapshot.
   *
   * @param yamlFilePath Path to the snapshot's YAML property file
   * @param key Property key
   * @throws IOException if an I/O error occurs
   * @throws OMException if snapshot doesn't exist or operation fails
   */
  void removeProperty(String yamlFilePath, String key)
      throws IOException, OMException;
}
