/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
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

import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;

import java.util.List;

/**
 * Used in {@link org.apache.hadoop.ozone.om.service.DirectoryDeletingService}
 * to capture the result of each delete task.
 */
public class DeleteKeysResult {

  private List<OmKeyInfo> keysToDelete;
  private long consumedSize;

  private boolean processedKeys;

  public DeleteKeysResult(List<OmKeyInfo> keysToDelete,
      long consumedSize, boolean processedKeys) {
    this.keysToDelete = keysToDelete;
    this.consumedSize = consumedSize;
    this.processedKeys = processedKeys;
  }

  public List<OmKeyInfo> getKeysToDelete() {
    return keysToDelete;
  }

  public long getConsumedSize() {
    return consumedSize;
  }

  public boolean isProcessedKeys() {
    return processedKeys;
  }
}
