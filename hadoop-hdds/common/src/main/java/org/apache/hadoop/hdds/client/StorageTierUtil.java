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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.scm.exceptions.SCMException;

/**
 * Utility class for managing StorageTier operations.
 */
public final class StorageTierUtil {
  private StorageTierUtil() {
  }

  /**
   * Validates the given StorageTier and throws an exception if it is empty.
   *
   * @param storageTier the StorageTier to check
   * @throws IllegalArgumentException if the StorageTier is null
   * @throws SCMException if the StorageTier is empty
   */
  public static void validateNotEmpty(StorageTier storageTier) throws SCMException {
    if (storageTier == null) {
      throw new IllegalArgumentException("storageTier must not be null");
    }
    if (storageTier.equals(StorageTier.EMPTY)) {
      throw new SCMException("Cannot create Pipeline for empty tier",
          SCMException.ResultCodes.CANNOT_CREATE_PIPELINE_FOR_EMPTY_TIER);
    }
  }

  public static List<StorageTier> findSupportedStorageTiers(
      List<Set<StorageType>> dnStorageTypes) {
    List<StorageTier> supportedStorageTiers = new ArrayList<>();
    if (dnStorageTypes.isEmpty()) {
      return supportedStorageTiers;
    }
    // We only support uniform storage tiers currently
    for (StorageTier storageTier : StorageTier.values()) {
      if (storageTier.equals(StorageTier.EMPTY)) {
        continue;
      }
      if (!storageTier.isUniform()) {
        throw new UnsupportedOperationException(storageTier + " is not a uniform storage tier");
      }
      boolean supportedTier = true;
      for (Set<StorageType> dnStorageType : dnStorageTypes) {
        if (!dnStorageType.contains(storageTier.getUniformStorageType())) {
          supportedTier = false;
          break;
        }
      }
      if (supportedTier) {
        supportedStorageTiers.add(storageTier);
      }
    }
    return supportedStorageTiers;
  }
}
