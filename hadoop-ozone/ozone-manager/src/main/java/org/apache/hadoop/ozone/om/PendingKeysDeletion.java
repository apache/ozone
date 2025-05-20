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

import java.util.List;
import java.util.Map;
import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;

/**
 * Tracks metadata for keys pending deletion and their associated blocks.
 *
 * This class maintains:
 * <ul>
 *   <li>A list of {@link BlockGroup} entries, where each entry contains
 *       a key name and its associated block IDs</li>
 *   <li>A key-value mapping that requires updating after the remaining
 *       blocks are purged</li>
 * </ul>
 */
public class PendingKeysDeletion {

  private Map<String, RepeatedOmKeyInfo> keysToModify;
  private List<BlockGroup> keyBlocksList;

  public PendingKeysDeletion(List<BlockGroup> keyBlocksList,
       Map<String, RepeatedOmKeyInfo> keysToModify) {
    this.keysToModify = keysToModify;
    this.keyBlocksList = keyBlocksList;
  }

  public Map<String, RepeatedOmKeyInfo> getKeysToModify() {
    return keysToModify;
  }

  public List<BlockGroup> getKeyBlocksList() {
    return keyBlocksList;
  }
}
