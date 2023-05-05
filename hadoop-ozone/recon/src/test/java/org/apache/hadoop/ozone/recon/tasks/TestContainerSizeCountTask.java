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

package org.apache.hadoop.ozone.recon.tasks;

import static org.hadoop.ozone.recon.schema.tables.ContainerCountBySizeTable.CONTAINER_COUNT_BY_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.ozone.recon.persistence.AbstractReconSqlDBTest;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;
import org.hadoop.ozone.recon.schema.UtilizationSchemaDefinition;
import org.hadoop.ozone.recon.schema.tables.daos.ContainerCountBySizeDao;
import org.hadoop.ozone.recon.schema.tables.daos.ReconTaskStatusDao;
import org.jooq.DSLContext;
import org.jooq.Record1;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Class to test the process method of ContainerSizeCountTask.
 */
public class TestContainerSizeCountTask extends AbstractReconSqlDBTest {

  private ContainerManager containerManager;
  private StorageContainerServiceProvider scmClient;
  private ReconTaskStatusDao reconTaskStatusDao;
  private ReconTaskConfig reconTaskConfig;
  private ContainerCountBySizeDao containerCountBySizeDao;
  private UtilizationSchemaDefinition utilizationSchemaDefinition;
  private ContainerSizeCountTask task;
  private DSLContext dslContext;

  @BeforeEach
  public void setUp() {
    utilizationSchemaDefinition =
        getSchemaDefinition(UtilizationSchemaDefinition.class);
    dslContext = utilizationSchemaDefinition.getDSLContext();
    containerCountBySizeDao = getDao(ContainerCountBySizeDao.class);
    reconTaskStatusDao = getDao(ReconTaskStatusDao.class);
    reconTaskConfig = new ReconTaskConfig();
    reconTaskConfig.setContainerSizeCountTaskInterval(Duration.ofSeconds(1));
    containerManager = mock(ContainerManager.class);
    scmClient = mock(StorageContainerServiceProvider.class);
    task = new ContainerSizeCountTask(
        containerManager,
        scmClient,
        reconTaskStatusDao,
        reconTaskConfig,
        containerCountBySizeDao,
        utilizationSchemaDefinition);
    // Truncate table before running each test
    dslContext.truncate(CONTAINER_COUNT_BY_SIZE);
  }


  @Test
  public void testProcess() {
    // Write 2 keys
    ContainerInfo omContainerInfo1 = mock(ContainerInfo.class);
    given(omContainerInfo1.containerID()).willReturn(new ContainerID(1));
    given(omContainerInfo1.getUsedBytes()).willReturn(1500000000L); // 1.5GB

    ContainerInfo omContainerInfo2 = mock(ContainerInfo.class);
    given(omContainerInfo2.containerID()).willReturn(new ContainerID(2));
    given(omContainerInfo2.getUsedBytes()).willReturn(2500000000L); // 2.5GB

    // mock getContainers method to return a list of containers
    List<ContainerInfo> containers = new ArrayList<>();
    containers.add(omContainerInfo1);
    containers.add(omContainerInfo2);

    task.process(containers);

    // Verify 2 containers are in correct bins.
    assertEquals(2, containerCountBySizeDao.count());

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

    // Add a new key
    ContainerInfo omContainerInfo3 = mock(ContainerInfo.class);
    given(omContainerInfo3.containerID()).willReturn(new ContainerID(3));
    given(omContainerInfo3.getUsedBytes()).willReturn(1000000000L); // 1GB
    containers.add(omContainerInfo3);

    // Update existing key.
    given(omContainerInfo2.containerID()).willReturn(new ContainerID(2));
    given(omContainerInfo2.getUsedBytes()).willReturn(50000L); // 50KB

    task.process(containers);

    // Total size groups added to the database
    assertEquals(4, containerCountBySizeDao.count());

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
    task.process(containers);
    recordToFind.value1(2147483648L);
    assertEquals(0, containerCountBySizeDao
        .findById(recordToFind.value1())
        .getCount()
        .longValue());
  }
}
