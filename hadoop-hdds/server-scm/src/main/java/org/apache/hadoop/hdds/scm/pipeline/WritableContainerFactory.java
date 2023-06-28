/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.pipeline;

import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.pipeline.WritableECContainerProvider.WritableECContainerProviderConfig;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.hdds.conf.StorageUnit.BYTES;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT;

/**
 * Factory class to obtain a container to which a block can be allocated for
 * write.
 */
public class WritableContainerFactory {

  private final WritableContainerProvider<ReplicationConfig> ratisProvider;
  private final WritableContainerProvider<ReplicationConfig> standaloneProvider;
  private final WritableContainerProvider<ECReplicationConfig> ecProvider;

  public WritableContainerFactory(StorageContainerManager scm) {
    ConfigurationSource conf = scm.getConfiguration();

    this.ratisProvider = new WritableRatisContainerProvider(
        conf, scm.getPipelineManager(),
        scm.getContainerManager(), scm.getPipelineChoosePolicy());
    this.standaloneProvider = ratisProvider;

    this.ecProvider = new WritableECContainerProvider(
        conf.getObject(WritableECContainerProviderConfig.class),
        getConfiguredContainerSize(conf),
        scm.getScmNodeManager(),
        scm.getPipelineManager(),
        scm.getContainerManager(),
        scm.getEcPipelineChoosePolicy());
  }

  public ContainerInfo getContainer(final long size,
      ReplicationConfig repConfig, String owner, ExcludeList excludeList)
      throws IOException, TimeoutException {
    switch (repConfig.getReplicationType()) {
    case STAND_ALONE:
      return standaloneProvider
          .getContainer(size, repConfig, owner, excludeList);
    case RATIS:
      return ratisProvider.getContainer(size, repConfig, owner, excludeList);
    case EC:
      return ecProvider.getContainer(size, (ECReplicationConfig)repConfig,
          owner, excludeList);
    default:
      throw new IOException(repConfig.getReplicationType()
          + " is an invalid replication type");
    }
  }

  private long getConfiguredContainerSize(ConfigurationSource conf) {
    return (long) conf.getStorageSize(OZONE_SCM_CONTAINER_SIZE,
        OZONE_SCM_CONTAINER_SIZE_DEFAULT, BYTES);
  }

}
