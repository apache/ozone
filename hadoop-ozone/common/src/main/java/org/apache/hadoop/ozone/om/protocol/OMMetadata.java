/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om.protocol;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.ozone.om.helpers.OMNodeDetails;

/**
 * Class storing the OM metadata such as the node details in memory and node
 * details when config is reloaded from disk.
 * Note that this class is used as a structure to transfer the OM node
 * information through the {@link OMMetadataProtocol} and not for storing the
 * metadata information in OzoneManager itself.
 */
public class OMMetadata {

  // OM nodes present in OM's memory
  private List<OMNodeDetails> omNodesInMemory = new ArrayList<>();
  // OM nodes reloaded from new config on disk
  private List<OMNodeDetails> omNodesInNewConf = new ArrayList<>();

  private OMMetadata(List<OMNodeDetails> inMemoryNodeList,
      List<OMNodeDetails> onDiskNodeList) {
    if (inMemoryNodeList != null) {
      this.omNodesInMemory.addAll(inMemoryNodeList);
    }
    if (onDiskNodeList != null) {
      this.omNodesInNewConf.addAll(onDiskNodeList);
    }
  }

  public static class Builder {
    private List<OMNodeDetails> omNodesInMemory;
    private List<OMNodeDetails> omNodesInNewConf;

    public Builder() {
      this.omNodesInMemory = new ArrayList<>();
      this.omNodesInNewConf = new ArrayList<>();
    }

    public Builder addToNodesInMemory(OMNodeDetails nodeDetails) {
     this.omNodesInMemory.add(nodeDetails);
      return this;
    }

    public Builder addToNodesInNewConf(OMNodeDetails nodeDetails) {
      this.omNodesInNewConf.add(nodeDetails);
      return this;
    }

    public OMMetadata build() {
      return new OMMetadata(omNodesInMemory, omNodesInNewConf);
    }
  }

  public List<OMNodeDetails> getOmNodesInNewConf() {
    return omNodesInNewConf;
  }
}
