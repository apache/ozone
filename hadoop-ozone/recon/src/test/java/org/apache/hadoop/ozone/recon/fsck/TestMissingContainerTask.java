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

package org.apache.hadoop.ozone.recon.fsck;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.ContainerReplicaProto.State;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.ozone.recon.persistence.AbstractSqlDatabaseTest;
import org.apache.hadoop.ozone.recon.persistence.ContainerSchemaManager;
import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade;
import org.apache.hadoop.test.LambdaTestUtils;
import org.hadoop.ozone.recon.schema.ContainerSchemaDefinition;
import org.hadoop.ozone.recon.schema.ReconTaskSchemaDefinition;
import org.hadoop.ozone.recon.schema.tables.daos.ContainerHistoryDao;
import org.hadoop.ozone.recon.schema.tables.daos.MissingContainersDao;
import org.hadoop.ozone.recon.schema.tables.daos.ReconTaskStatusDao;
import org.hadoop.ozone.recon.schema.tables.pojos.MissingContainers;
import org.hadoop.ozone.recon.schema.tables.pojos.ReconTaskStatus;
import org.jooq.Configuration;
import org.junit.Assert;
import org.junit.Test;

/**
 * Class to test single run of Missing Container Task.
 */
public class TestMissingContainerTask extends AbstractSqlDatabaseTest {

  @Test
  public void testRun() throws Exception {
    Configuration sqlConfiguration =
        getInjector().getInstance((Configuration.class));

    ReconTaskSchemaDefinition taskSchemaDefinition = getInjector().getInstance(
        ReconTaskSchemaDefinition.class);
    taskSchemaDefinition.initializeSchema();

    ContainerSchemaDefinition schemaDefinition =
        getInjector().getInstance(ContainerSchemaDefinition.class);
    schemaDefinition.initializeSchema();

    MissingContainersDao missingContainersTableHandle =
        new MissingContainersDao(sqlConfiguration);

    ContainerSchemaManager containerSchemaManager =
        new ContainerSchemaManager(mock(ContainerHistoryDao.class),
            schemaDefinition, missingContainersTableHandle);
    ReconStorageContainerManagerFacade scmMock =
        mock(ReconStorageContainerManagerFacade.class);
    ContainerManager containerManagerMock = mock(ContainerManager.class);
    ContainerReplica unhealthyReplicaMock = mock(ContainerReplica.class);
    when(unhealthyReplicaMock.getState()).thenReturn(State.UNHEALTHY);
    ContainerReplica healthyReplicaMock = mock(ContainerReplica.class);
    when(healthyReplicaMock.getState()).thenReturn(State.CLOSED);
    when(scmMock.getContainerManager()).thenReturn(containerManagerMock);
    when(containerManagerMock.getContainerIDs())
        .thenReturn(getMockContainerIDs(3));
    // return one HEALTHY and one UNHEALTHY replica for container ID 1
    when(containerManagerMock.getContainerReplicas(new ContainerID(1L)))
        .thenReturn(Collections.unmodifiableSet(
            new HashSet<>(
                Arrays.asList(healthyReplicaMock, unhealthyReplicaMock)
            )));
    // return one UNHEALTHY replica for container ID 2
    when(containerManagerMock.getContainerReplicas(new ContainerID(2L)))
        .thenReturn(Collections.singleton(unhealthyReplicaMock));
    // return 0 replicas for container ID 3
    when(containerManagerMock.getContainerReplicas(new ContainerID(3L)))
        .thenReturn(Collections.emptySet());

    List<MissingContainers> all = missingContainersTableHandle.findAll();
    Assert.assertTrue(all.isEmpty());

    long currentTime = System.currentTimeMillis();
    ReconTaskStatusDao reconTaskStatusDao =
        new ReconTaskStatusDao(sqlConfiguration);
    MissingContainerTask missingContainerTask =
        new MissingContainerTask(scmMock, reconTaskStatusDao,
            containerSchemaManager);
    missingContainerTask.register();
    missingContainerTask.start();

    LambdaTestUtils.await(6000, 1000, () ->
        (containerSchemaManager.getAllMissingContainers().size() == 2));

    all = containerSchemaManager.getAllMissingContainers();
    // Container IDs 2 and 3 should be present in the missing containers table
    Set<Long> missingContainerIDs = Collections.unmodifiableSet(
        new HashSet<>(Arrays.asList(2L, 3L))
    );
    Assert.assertTrue(all.stream().allMatch(r ->
        missingContainerIDs.contains(r.getContainerId())));
    ReconTaskStatus taskStatus =
        reconTaskStatusDao.findById(missingContainerTask.getTaskName());
    Assert.assertTrue(taskStatus.getLastUpdatedTimestamp() >
        currentTime);
  }

  private Set<ContainerID> getMockContainerIDs(int num) {
    Set<ContainerID> containerIDs = new HashSet<>();
    for (int i = 1; i <= num; i++) {
      containerIDs.add(new ContainerID(i));
    }
    return containerIDs;
  }
}