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

import java.util.Collections;
import java.util.List;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used in {@link org.apache.hadoop.ozone.om.service.DirectoryDeletingService}
 * to capture the result of each delete task.
 */
public class DeleteKeysResult {

  private List<OmKeyInfo> keysToDelete;
  private boolean processedKeys;
  private List<ExclusiveRange> keyRanges;

  DeleteKeysResult(List<OmKeyInfo> keysToDelete, List<ExclusiveRange> keyRanges, boolean processedKeys) {
    this.keysToDelete = Collections.unmodifiableList(keysToDelete);
    this.processedKeys = processedKeys;
    this.keyRanges = Collections.unmodifiableList(keyRanges);
  }

  public List<OmKeyInfo> getKeysToDelete() {
    return keysToDelete;
  }

  public boolean isProcessedKeys() {
    return processedKeys;
  }

  public List<ExclusiveRange> getKeyRanges() {
    return keyRanges;
  }

  /**
   * Represents a half-open key range {@code [startKey, exclusiveEndKey)} used
   * for RocksDB deleteRange operations.
   */
  public static class ExclusiveRange {
    private final String startKey;
    private final String exclusiveEndKey;

    public ExclusiveRange(String startKey, String exclusiveEndKey) {
      this.startKey = startKey;
      this.exclusiveEndKey = exclusiveEndKey;
    }

    public String getExclusiveEndKey() {
      return exclusiveEndKey;
    }

    public String getStartKey() {
      return startKey;
    }
  }

}
