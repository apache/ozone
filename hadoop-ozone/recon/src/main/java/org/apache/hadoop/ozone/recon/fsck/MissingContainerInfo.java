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

package org.apache.hadoop.ozone.recon.fsck;

import java.util.List;
import org.apache.hadoop.ozone.recon.api.types.KeyMetadata;

/**
 * Class to encapsulate the information of a single missing Container. This
 * will be used in the missing container API response payload.
 */
public class MissingContainerInfo {

  private long containerId;
  private long missingSinceTimestamp;
  private String lastKnownPipelineId;
  private List<String> lastKnownDatanodes;
  private List<KeyMetadata> keysInContainer;

  public MissingContainerInfo(long containerId,
                              long missingSinceTimestamp,
                              String lastKnownPipelineId,
                              List<String> lastKnownDatanodes,
                              List<KeyMetadata> keysInContainer) {
    this.containerId = containerId;
    this.missingSinceTimestamp = missingSinceTimestamp;
    this.lastKnownPipelineId = lastKnownPipelineId;
    this.lastKnownDatanodes = lastKnownDatanodes;
    this.keysInContainer = keysInContainer;
  }

  public long getContainerId() {
    return containerId;
  }

  public long getMissingSinceTimestamp() {
    return missingSinceTimestamp;
  }

  public String getLastKnownPipelineId() {
    return lastKnownPipelineId;
  }

  public List<String> getLastKnownDatanodes() {
    return lastKnownDatanodes;
  }

  public List<KeyMetadata> getKeysInContainer() {
    return keysInContainer;
  }
}
