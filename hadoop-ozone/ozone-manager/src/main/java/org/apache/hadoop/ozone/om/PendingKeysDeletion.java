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
 *
 */
package org.apache.hadoop.ozone.om;

import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;

import java.util.HashMap;
import java.util.List;

/**
 * Return class for OMMetadataManager#getPendingDeletionKeys.
 */
public class PendingKeysDeletion {

  private HashMap<String, RepeatedOmKeyInfo> keysToModify;
  private List<BlockGroup> keyBlocksList;

  public PendingKeysDeletion(List<BlockGroup> keyBlocksList,
       HashMap<String, RepeatedOmKeyInfo> keysToModify) {
    this.keysToModify = keysToModify;
    this.keyBlocksList = keyBlocksList;
  }

  public HashMap<String, RepeatedOmKeyInfo> getKeysToModify() {
    return keysToModify;
  }

  public List<BlockGroup> getKeyBlocksList() {
    return keyBlocksList;
  }
}
