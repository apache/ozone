/*
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

package org.apache.hadoop.ozone.debug;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;

/**
 * Class that gives container and chunk Information.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ContainerChunkInfo {
  private String containerPath;
  private List<ChunkDetails> chunkInfos;
  private HashSet<String> files;
  private UUID pipelineID;
  private Pipeline pipeline;

  public void setFiles(HashSet<String> files) {
    this.files = files;
  }

  public void setPipelineID(UUID pipelineID) {
    this.pipelineID = pipelineID;
  }

  public Pipeline getPipeline() {
    return pipeline;
  }

  public void setPipeline(Pipeline pipeline) {
    this.pipeline = pipeline;
  }

  public void setContainerPath(String containerPath) {
    this.containerPath = containerPath;
  }

  public void setChunkInfos(List<ChunkDetails> chunkInfos) {
    this.chunkInfos = chunkInfos;
  }


  @Override
  public String toString() {
    return "Container{"
            + "containerPath='"
            + containerPath
            + '\''
            + ", chunkInfos="
            + chunkInfos
            + ", pipeline="
            + pipeline
            + '}'
            + "files="
            + files
            + "PipelineID="
            + pipelineID;
  }
}
