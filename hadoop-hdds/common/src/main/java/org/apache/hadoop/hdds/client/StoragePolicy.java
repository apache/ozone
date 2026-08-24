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

package org.apache.hadoop.hdds.client;

/**
 * Interface for storage policies that define how to select storage tiers for data replication.
 *
 * <p>A storage policy specifies the preferred and fallback storage tiers for placing
 * block replicas.
 */
public interface StoragePolicy {

  /**
   * Retrieves the name of the storage policy.
   *
   * @return a string representing the name of the storage policy.
   */
  String getName();

  /**
   * Retrieves the preferred storage tier used for placing data replicas.
   *
   * <p>This is the preferred storage tier where new data is initially stored
   * according to the specified storage policy.
   *
   * @return the default {@link StorageTier} used for data placement.
   */
  StorageTier getCreationTier();

  /**
   * Retrieves the fallback storage tier used during the creation of new data replicas.
   *
   * <p>If the preferred storage tier is unavailable, this fallback tier is used to
   * ensure that new data can still be reliably stored.
   *
   * @return the fallback {@link StorageTier} used for data placement.
   */
  StorageTier getCreationFallbackTier();
}
