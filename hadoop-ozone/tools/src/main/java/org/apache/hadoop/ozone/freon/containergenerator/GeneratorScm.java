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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.freon.containergenerator;

import java.util.concurrent.Callable;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.hdds.utils.db.Table;

import com.codahale.metrics.Timer;
import static org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition.CONTAINERS;
import picocli.CommandLine.Command;


/**
 * Container generator for SCM metadata.
 */
@Command(name = "cgscm",
    description = "Offline container metadata generator for Storage Conainer "
        + "Manager",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true,
    showDefaultValues = true)
public class GeneratorScm extends BaseGenerator {

  private DBStore scmDb;

  private Table<ContainerID, ContainerInfo> containerStore;

  private Timer timer;

  @Override
  public Void call() throws Exception {
    init();

    ConfigurationSource config = createOzoneConfiguration();

    scmDb = DBStoreBuilder.createDBStore(config, new SCMDBDefinition());

    containerStore = CONTAINERS.getTable(scmDb);

    timer = getMetrics().timer("scm-generator");
    runTests(this::writeScmData);
    scmDb.close();
    return null;
  }


  private void writeScmData(long index) throws Exception {
    timer.time((Callable<Void>) () -> {
      long containerId = getContainerIdOffset() + index;
      // SCM
      ContainerInfo containerInfo =
          new ContainerInfo.Builder()
              .setContainerID(containerId)
              .setState(LifeCycleState.CLOSED)
              .setReplicationFactor(ReplicationFactor.THREE)
              .setReplicationType(ReplicationType.STAND_ALONE)
              .setOwner(getUserId())
              .build();

      containerStore.put(new ContainerID(containerId), containerInfo);
      return null;
    });

  }

}
