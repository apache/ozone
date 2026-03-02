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

package org.apache.hadoop.hdds.scm.pipeline;

import java.io.IOException;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;

/**
 * Interface used by the WritableContainerFactory to obtain a writable container
 * from the providers. This interface is implemented by various
 * WritableContainerProviders, eg Ratis and Standalone. These providers
 * will query the open pipelines from the PipelineManager and select or allocate
 * a container on the pipeline to allow for a new block to be created in it.
 *
 * The provider can also manage the number of open pipelines, including asking
 * the pipeline manager to create a new pipeline if needed, or close a pipeline.
 *
 * Anytime a container needs to be selected for a new block, this interface
 * should be used via the WritableContainerFactory instance.
 */
public interface WritableContainerProvider<T extends ReplicationConfig> {

  /**
   *
   * @param size The max size of block in bytes which will be written
   * @param repConfig The replication Config indicating if the container should
   *                  be Ratis or Standalone.
   * @param owner The owner of the container
   * @param excludeList A set of datanodes, container and pipelines which should
   *                    not be considered.
   * @return A ContainerInfo which is open and has the capacity to store the
   *         desired block size.
   * @throws IOException
   */
  ContainerInfo getContainer(long size, T repConfig,
      String owner, ExcludeList excludeList)
      throws IOException;

  default ContainerInfo getContainer(long size, T repConfig,
      String owner, ExcludeList excludeList, StorageType storageType)
      throws IOException {
    return getContainer(size, repConfig, owner, excludeList);
  }

}
