/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.recon.spi;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.ozone.recon.api.types.ContainerKeyPrefix;
import org.apache.hadoop.ozone.recon.api.types.ContainerMetadata;
import org.apache.hadoop.utils.db.TableIterator;

/**
 * The Recon Container DB Service interface.
 */
@InterfaceStability.Unstable
public interface ContainerDBServiceProvider {

  /**
   * Create new container DB and bulk Store the container to Key prefix
   * mapping.
   * @param containerKeyPrefixCounts Map of containerId, key-prefix tuple to
   *                                 key count.
   */
  void initNewContainerDB(Map<ContainerKeyPrefix, Integer>
                                    containerKeyPrefixCounts)
      throws IOException;

  /**
   * Store the container to Key prefix mapping into the Recon Container DB.
   *
   * @param containerKeyPrefix the containerId, key-prefix tuple.
   * @param count              Count of Keys with that prefix.
   */
  void storeContainerKeyMapping(ContainerKeyPrefix containerKeyPrefix,
                                Integer count) throws IOException;

  /**
   * Get the stored key prefix count for the given containerId, key prefix.
   *
   * @param containerKeyPrefix the containerId, key-prefix tuple.
   * @return count of keys with that prefix.
   */
  Integer getCountForForContainerKeyPrefix(
      ContainerKeyPrefix containerKeyPrefix) throws IOException;

  /**
   * Get the stored key prefixes for the given containerId.
   *
   * @param containerId the given containerId.
   * @return Map of Key prefix -> count.
   */
  Map<ContainerKeyPrefix, Integer> getKeyPrefixesForContainer(
      long containerId) throws IOException;

  /**
   * Get the stored key prefixes for the given containerId starting
   * after the given keyPrefix.
   *
   * @param containerId the given containerId.
   * @param prevKeyPrefix the key prefix to seek to and start scanning.
   * @return Map of Key prefix -> count.
   */
  Map<ContainerKeyPrefix, Integer> getKeyPrefixesForContainer(
      long containerId, String prevKeyPrefix) throws IOException;

  /**
   * Get a Map of containerID, containerMetadata of Containers only for the
   * given limit. If the limit is -1 or any integer <0, then return all
   * the containers without any limit.
   *
   * @param limit the no. of containers to fetch.
   * @param prevContainer containerID after which the results are returned.
   * @return Map of containerID -> containerMetadata.
   * @throws IOException
   */
  Map<Long, ContainerMetadata> getContainers(int limit, long prevContainer)
      throws IOException;

  /**
   * Delete an entry in the container DB.
   * @param containerKeyPrefix container key prefix to be deleted.
   * @throws IOException exception.
   */
  void deleteContainerMapping(ContainerKeyPrefix containerKeyPrefix)
      throws IOException;

  /**
   * Get iterator to the entire container DB.
   * @return TableIterator
   */
  TableIterator getContainerTableIterator();
}
