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

package org.apache.hadoop.ozone.recon.tasks;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.CLOSED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.CLOSING;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.DELETED;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.OPEN;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState.QUASI_CLOSED;
import static org.apache.ozone.recon.schema.generated.tables.ContainerCountBySizeTable.CONTAINER_COUNT_BY_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.ozone.recon.persistence.AbstractReconSqlDBTest;
import org.apache.hadoop.ozone.recon.tasks.updater.ReconTaskStatusUpdater;
import org.apache.hadoop.ozone.recon.tasks.updater.ReconTaskStatusUpdaterManager;
import org.apache.ozone.recon.schema.UtilizationSchemaDefinition;
import org.apache.ozone.recon.schema.generated.tables.daos.ContainerCountBySizeDao;
import org.apache.ozone.recon.schema.generated.tables.daos.ReconTaskStatusDao;
import org.jooq.DSLContext;
import org.jooq.Record1;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Class to test the process method of ContainerSizeCountTask.
 */
public class TestContainerSizeCountTask extends AbstractReconSqlDBTest {

  private ContainerCountBySizeDao containerCountBySizeDao;
  private ContainerSizeCountTask task;
  private DSLContext dslContext;

  public TestContainerSizeCountTask() {
    super();
  }

  @BeforeEach
  public void setUp() {
    UtilizationSchemaDefinition utilizationSchemaDefinition = getSchemaDefinition(UtilizationSchemaDefinition.class);
    dslContext = utilizationSchemaDefinition.getDSLContext();
    containerCountBySizeDao = getDao(ContainerCountBySizeDao.class);
    ReconTaskConfig reconTaskConfig = new ReconTaskConfig();
    reconTaskConfig.setContainerSizeCountTaskInterval(Duration.ofSeconds(1));
    ReconTaskStatusUpdaterManager reconTaskStatusUpdaterManager = mock(ReconTaskStatusUpdaterManager.class);
    when(reconTaskStatusUpdaterManager.getTaskStatusUpdater(anyString())).thenReturn(new ReconTaskStatusUpdater(
        getDao(ReconTaskStatusDao.class), "mockedTask-" + System.currentTimeMillis()));
    ContainerManager containerManager = mock(ContainerManager.class);
    task = new ContainerSizeCountTask(
        containerManager,
        reconTaskConfig,
        containerCountBySizeDao,
        utilizationSchemaDefinition,
        reconTaskStatusUpdaterManager);
    // Truncate table before running each test
    dslContext.truncate(CONTAINER_COUNT_BY_SIZE);
  }

  @Test
  public void testProcess() {
    // mock a container with invalid used bytes
    ContainerInfo omContainerInfo0 = mock(ContainerInfo.class);
    given(omContainerInfo0.containerID()).willReturn(ContainerID.valueOf(0));
    given(omContainerInfo0.getUsedBytes()).willReturn(-1L);
    given(omContainerInfo0.getState()).willReturn(OPEN);

    // Write 2 keys
    ContainerInfo omContainerInfo1 = mock(ContainerInfo.class);
    given(omContainerInfo1.containerID()).willReturn(ContainerID.valueOf(1));
    given(omContainerInfo1.getUsedBytes()).willReturn(1500000000L); // 1.5GB
    given(omContainerInfo1.getState()).willReturn(CLOSED);

    ContainerInfo omContainerInfo2 = mock(ContainerInfo.class);
    given(omContainerInfo2.containerID()).willReturn(ContainerID.valueOf(2));
    given(omContainerInfo2.getUsedBytes()).willReturn(2500000000L); // 2.5GB
    given(omContainerInfo2.getState()).willReturn(CLOSING);

    // mock getContainers method to return a list of containers
    List<ContainerInfo> containers = new ArrayList<>();
    containers.add(omContainerInfo0);
    containers.add(omContainerInfo1);
    containers.add(omContainerInfo2);

    task.processContainers(containers);

    // Verify 3 containers are in correct bins.
    assertEquals(3, containerCountBySizeDao.count());

    // container size upper bound for
    // 1500000000L (1.5GB) is 2147483648L = 2^31 = 2GB (next highest power of 2)
    Record1<Long> recordToFind =
        dslContext.newRecord(
                CONTAINER_COUNT_BY_SIZE.CONTAINER_SIZE)
            .value1(2147483648L);
    assertEquals(1L,
        containerCountBySizeDao.findById(recordToFind.value1()).getCount()
            .longValue());
    // container size upper bound for
    // 2500000000L (2.5GB) is 4294967296L = 2^32 = 4GB (next highest power of 2)
    recordToFind.value1(4294967296L);
    assertEquals(1L,
        containerCountBySizeDao.findById(recordToFind.value1()).getCount()
            .longValue());

    // Add a new container
    ContainerInfo omContainerInfo3 = mock(ContainerInfo.class);
    given(omContainerInfo3.containerID()).willReturn(ContainerID.valueOf(3));
    given(omContainerInfo3.getUsedBytes()).willReturn(1000000000L); // 1GB
    given(omContainerInfo3.getState()).willReturn(QUASI_CLOSED);
    containers.add(omContainerInfo3);

    // Update existing key.
    given(omContainerInfo2.containerID()).willReturn(ContainerID.valueOf(2));
    given(omContainerInfo2.getUsedBytes()).willReturn(50000L); // 50KB

    task.processContainers(containers);

    // Total size groups added to the database
    assertEquals(5, containerCountBySizeDao.count());

    // Check whether container size upper bound for
    // 50000L is 536870912L = 2^29 = 512MB (next highest power of 2)
    recordToFind.value1(536870912L);
    assertEquals(1, containerCountBySizeDao
        .findById(recordToFind.value1())
        .getCount()
        .longValue());

    // Check whether container size of 1000000000L has been successfully updated
    // The previous value upperbound was 4294967296L which is no longer there
    recordToFind.value1(4294967296L);
    assertEquals(0, containerCountBySizeDao
        .findById(recordToFind.value1())
        .getCount()
        .longValue());

    // Remove the container having size 1.5GB and upperbound 2147483648L
    containers.remove(omContainerInfo1);
    task.processContainers(containers);
    recordToFind.value1(2147483648L);
    assertEquals(0, containerCountBySizeDao
        .findById(recordToFind.value1())
        .getCount()
        .longValue());
  }

  @Test
  public void testProcessDeletedAndNegativeSizedContainers() {
    // Create a list of containers, including one that is deleted
    ContainerInfo omContainerInfo1 = mock(ContainerInfo.class);
    given(omContainerInfo1.containerID()).willReturn(ContainerID.valueOf(1));
    given(omContainerInfo1.getUsedBytes()).willReturn(1500000000L); // 1.5GB
    given(omContainerInfo1.getState()).willReturn(OPEN);

    ContainerInfo omContainerInfo2 = mock(ContainerInfo.class);
    given(omContainerInfo2.containerID()).willReturn(ContainerID.valueOf(2));
    given(omContainerInfo2.getUsedBytes()).willReturn(2500000000L); // 2.5GB
    given(omContainerInfo2.getState()).willReturn(CLOSED);

    ContainerInfo omContainerInfoDeleted = mock(ContainerInfo.class);
    given(omContainerInfoDeleted.containerID()).willReturn(ContainerID.valueOf(3));
    given(omContainerInfoDeleted.getUsedBytes()).willReturn(1000000000L);
    given(omContainerInfoDeleted.getState()).willReturn(DELETED); // 1GB

    // Create a mock container with negative size
    final ContainerInfo negativeSizeContainer = mock(ContainerInfo.class);
    given(negativeSizeContainer.containerID()).willReturn(ContainerID.valueOf(0));
    given(negativeSizeContainer.getUsedBytes()).willReturn(-1L);
    given(negativeSizeContainer.getState()).willReturn(OPEN);

    // Create a mock container with negative size and DELETE state
    final ContainerInfo negativeSizeDeletedContainer =
        mock(ContainerInfo.class);
    given(negativeSizeDeletedContainer.containerID()).willReturn(
        ContainerID.valueOf(0));
    given(negativeSizeDeletedContainer.getUsedBytes()).willReturn(-1L);
    given(negativeSizeDeletedContainer.getState()).willReturn(DELETED);

    // Create a mock container with id 1 and updated size of 1GB from 1.5GB
    final ContainerInfo validSizeContainer = mock(ContainerInfo.class);
    given(validSizeContainer.containerID()).willReturn(ContainerID.valueOf(1));
    given(validSizeContainer.getUsedBytes()).willReturn(1000000000L); // 1GB
    given(validSizeContainer.getState()).willReturn(CLOSED);

    // Mock getContainers method to return a list of containers including
    // both valid and invalid ones
    List<ContainerInfo> containers = new ArrayList<>();
    containers.add(omContainerInfo1);
    containers.add(omContainerInfo2);
    containers.add(omContainerInfoDeleted);
    containers.add(negativeSizeContainer);
    containers.add(negativeSizeDeletedContainer);
    containers.add(validSizeContainer);

    task.processContainers(containers);

    // Verify that only the valid containers are counted
    assertEquals(3, containerCountBySizeDao.count());
  }

}
