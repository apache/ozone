package org.apache.hadoop.ozone.recon.tasks;

import static org.hadoop.ozone.recon.schema.tables.ContainerCountBySizeTable.CONTAINER_COUNT_BY_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
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
  public void testReprocess() {
    ContainerInfo omContainerInfo1 = mock(ContainerInfo.class);
    given(omContainerInfo1.containerID()).willReturn(new ContainerID(1));
    given(omContainerInfo1.getUsedBytes()).willReturn(1000L);

    ContainerInfo omContainerInfo2 = mock(ContainerInfo.class);
    given(omContainerInfo2.containerID()).willReturn(new ContainerID(2));
    given(omContainerInfo2.getUsedBytes()).willReturn(100000L);

    ContainerInfo omContainerInfo3 = mock(ContainerInfo.class);
    given(omContainerInfo3.containerID()).willReturn(new ContainerID(3));
    given(omContainerInfo3.getUsedBytes()).willReturn(1125899906842624L * 4); // 4PB

    // mock getContainers method to return a list of containers
    List<ContainerInfo> containers = new ArrayList<>();
    containers.add(omContainerInfo1);
    containers.add(omContainerInfo2);
    containers.add(omContainerInfo3);

    task.reprocess(containers);
    assertEquals(3, containerCountBySizeDao.count());

    Record1<Long> recordToFind =
        dslContext.newRecord(
                CONTAINER_COUNT_BY_SIZE.CONTAINER_SIZE)
            .value1(1024L);
    assertEquals(1L,
        containerCountBySizeDao.findById(recordToFind.value1()).getCount().longValue());
    // container size upper bound for 100000L is 131072L (next highest power of 2)
    recordToFind.value1(131072L);
    assertEquals(1L,
        containerCountBySizeDao.findById(recordToFind.value1()).getCount().longValue());
    // container size upper bound for 4PB is Long.MAX_VALUE
    recordToFind.value1(Long.MAX_VALUE);
    assertEquals(1L,
        containerCountBySizeDao.findById(recordToFind.value1()).getCount().longValue());
  }


  @Test
  public void testProcess() {
    // Write 2 keys
    ContainerInfo omContainerInfo1 = mock(ContainerInfo.class);
    given(omContainerInfo1.containerID()).willReturn(new ContainerID(1));
    given(omContainerInfo1.getUsedBytes()).willReturn(2000L);

    ContainerInfo omContainerInfo2 = mock(ContainerInfo.class);
    given(omContainerInfo2.containerID()).willReturn(new ContainerID(2));
    given(omContainerInfo2.getUsedBytes()).willReturn(10000L);

    // mock getContainers method to return a list of containers
    List<ContainerInfo> containers = new ArrayList<>();
    containers.add(omContainerInfo1);
    containers.add(omContainerInfo2);

    task.process(containers);

    // Verify 2 containers are in correct bins.
    assertEquals(2, containerCountBySizeDao.count());

    Record1<Long> recordToFind =
        dslContext.newRecord(
                CONTAINER_COUNT_BY_SIZE.CONTAINER_SIZE)
            .value1(2048L);
    assertEquals(1L,
        containerCountBySizeDao.findById(recordToFind.value1()).getCount()
            .longValue());
    // container size upper bound for 10000L is 16384L (next highest power of 2)
    recordToFind.value1(16384L);
    assertEquals(1L,
        containerCountBySizeDao.findById(recordToFind.value1()).getCount()
            .longValue());

    // Add a new key
    ContainerInfo omContainerInfo3 = mock(ContainerInfo.class);
    given(omContainerInfo3.containerID()).willReturn(new ContainerID(3));
    given(omContainerInfo3.getUsedBytes()).willReturn(1000L);
    containers.add(omContainerInfo3);

    // Update existing key.
    given(omContainerInfo2.containerID()).willReturn(new ContainerID(2));
    given(omContainerInfo2.getUsedBytes()).willReturn(50000L);

    task.process(containers);

    // Total size groups added to the database
    assertEquals(4, containerCountBySizeDao.count());

    // Check whether container size upper bound for 50000L is 65536L
    // (next highest power of 2)
    recordToFind.value1(65536L);
    assertEquals(1, containerCountBySizeDao
        .findById(recordToFind.value1())
        .getCount()
        .longValue());

    // Check whether container size of 10000L has been successfully updated
    recordToFind.value1(16384L);
    assertEquals(0, containerCountBySizeDao
        .findById(recordToFind.value1())
        .getCount()
        .longValue());

    // Remove the container having size 2000L
    containers.remove(omContainerInfo1);
    task.process(containers);
    recordToFind.value1(2048L);
    assertEquals(0, containerCountBySizeDao
        .findById(recordToFind.value1())
        .getCount()
        .longValue());
  }
}