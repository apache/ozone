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

import java.util.List;
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
   * @throws SCMException if the StorageTier is empty
   */
  public static void validateNotEmpty(StorageTier storageTier) throws SCMException {
    if (storageTier.equals(StorageTier.EMPTY)) {
      throw new SCMException("Cannot create Pipeline for empty tier",
          SCMException.ResultCodes.CANNOT_CREATE_PIPELINE_FOR_EMPTY_TIER);
    }
  }

  /**
   * Returns the StorageType for uniform StorageTier.
   *
   * @param storageTier the StorageTier to get StorageType from
   * @return The uniform StorageTier corresponding StorageType
   * @throws SCMException if the StorageTier is non-uniform or the EMPTY StorageTier
   */
  public static StorageType getStorageTypeForUniformStorageTier(StorageTier storageTier, ReplicationConfig config)
      throws SCMException {
    validateNotEmpty(storageTier);
    List<StorageType> storageTypes = storageTier.getStorageTypes(config.getRequiredNodes());
    if (storageTier.isUniform()) {
      return storageTypes.get(0);
    } else {
      throw new SCMException("Unsupported non-uniform storage tier " + storageTier,
          SCMException.ResultCodes.UNSUPPORTED_NON_UNIFORM_STORAGE_TIER);
    }
  }
}
