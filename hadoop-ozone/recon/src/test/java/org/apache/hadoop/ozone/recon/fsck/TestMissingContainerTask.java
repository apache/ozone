/**
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

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.ozone.recon.persistence.AbstractSqlDatabaseTest;
import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade;
import org.hadoop.ozone.recon.schema.ReconTaskSchemaDefinition;
import org.hadoop.ozone.recon.schema.UtilizationSchemaDefinition;
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
  public void testRun() throws IOException, SQLException, InterruptedException {
    Configuration sqlConfiguration =
        getInjector().getInstance((Configuration.class));

    ReconTaskSchemaDefinition taskSchemaDefinition = getInjector().getInstance(
        ReconTaskSchemaDefinition.class);
    taskSchemaDefinition.initializeSchema();

    UtilizationSchemaDefinition schemaDefinition =
        getInjector().getInstance(UtilizationSchemaDefinition.class);
    schemaDefinition.initializeSchema();

    ReconStorageContainerManagerFacade scmMock =
        mock(ReconStorageContainerManagerFacade.class);
    ContainerManager containerManagerMock = mock(ContainerManager.class);
    when(scmMock.getContainerManager()).thenReturn(containerManagerMock);
    when(containerManagerMock.getContainerIDs())
        .thenReturn(getMockContainerIDs(3));
    when(containerManagerMock.getContainerReplicas(new ContainerID(1L)))
        .thenReturn(Collections.singleton(mock(ContainerReplica.class)));
    when(containerManagerMock.getContainerReplicas(new ContainerID(2L)))
        .thenReturn(Collections.singleton(mock(ContainerReplica.class)));
    when(containerManagerMock.getContainerReplicas(new ContainerID(3L)))
        .thenReturn(Collections.emptySet());

    MissingContainersDao missingContainersTableHandle =
        new MissingContainersDao(sqlConfiguration);
    List<MissingContainers> all = missingContainersTableHandle.findAll();
    Assert.assertTrue(all.isEmpty());

    long currentTime = System.currentTimeMillis();
    ReconTaskStatusDao reconTaskStatusDao =
        new ReconTaskStatusDao(sqlConfiguration);
    MissingContainersDao missingContainersDao =
        new MissingContainersDao(sqlConfiguration);
    MissingContainerTask missingContainerTask =
        new MissingContainerTask(scmMock, reconTaskStatusDao,
            missingContainersDao);
    missingContainerTask.register();
    missingContainerTask.start();
    Thread.sleep(5000L);

    all = missingContainersTableHandle.findAll();
    Assert.assertEquals(1, all.size());
    Assert.assertEquals(3, all.get(0).getContainerId().longValue());

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