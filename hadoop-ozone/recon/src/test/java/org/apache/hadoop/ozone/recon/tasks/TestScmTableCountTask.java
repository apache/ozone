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


import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.ozone.recon.ReconTestInjector;
import org.apache.hadoop.ozone.recon.persistence.AbstractReconSqlDBTest;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;
import org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl;
import org.apache.hadoop.ozone.recon.spi.impl.StorageContainerServiceProviderImpl;
import org.hadoop.ozone.recon.schema.UtilizationSchemaDefinition;
import org.hadoop.ozone.recon.schema.tables.daos.ReconTaskStatusDao;
import org.hadoop.ozone.recon.schema.tables.daos.ScmTableCountDao;
import org.jooq.DSLContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import static org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition.DELETED_BLOCKS;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getTestReconOmMetadataManager;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.initializeNewOmMetadataManager;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;

public class TestScmTableCountTask extends AbstractReconSqlDBTest {

  private DBStore scmDBStore;
  private ScmTableCountDao scmTableCountDao;
  private ScmTableCountTask scmTableCountTask;
  private ReconTaskStatusDao reconTaskStatusDao;
  private ReconTaskConfig reconTaskConfig;
  private DSLContext dslContext;
  private boolean isSetupDone = false;
  private ReconOMMetadataManager reconOMMetadataManager;
  private ReconStorageContainerManagerFacade reconStorageContainerManager;
  private UtilizationSchemaDefinition utilizationSchemaDefinition;

  private void initializeInjector() throws IOException {
    this.reconOMMetadataManager = getTestReconOmMetadataManager(
        initializeNewOmMetadataManager(temporaryFolder.newFolder()),
        temporaryFolder.newFolder());

    ReconTestInjector reconTestInjector =
        new ReconTestInjector.Builder(temporaryFolder)
            .withReconSqlDb()
            .withReconOm(reconOMMetadataManager)
            .withOmServiceProvider(mock(OzoneManagerServiceProviderImpl.class))
            .addBinding(OzoneStorageContainerManager.class,
                ReconStorageContainerManagerFacade.class)
            .withContainerDB()
            .addBinding(StorageContainerServiceProvider.class,
                mock(StorageContainerServiceProviderImpl.class))
            .build();

    this.reconStorageContainerManager =
        reconTestInjector.getInstance(ReconStorageContainerManagerFacade.class);
    this.scmDBStore = reconStorageContainerManager.getScmDBStore();
    this.scmTableCountDao = getDao(ScmTableCountDao.class);
    this.reconTaskStatusDao = getDao(ReconTaskStatusDao.class);
    this.dslContext = getDslContext();
    this.reconTaskConfig = new ReconTaskConfig();
    this.reconTaskConfig.setContainerSizeCountTaskInterval(
        Duration.ofSeconds(1));
    this.utilizationSchemaDefinition =
        getSchemaDefinition(UtilizationSchemaDefinition.class);
    this.dslContext = utilizationSchemaDefinition.getDSLContext();
    this.scmTableCountTask =
        new ScmTableCountTask(reconStorageContainerManager,
            reconTaskStatusDao, reconTaskConfig,
            scmTableCountDao, utilizationSchemaDefinition);
  }

  @BeforeEach
  public void setUp() throws Exception {
    // The following setup runs only once
    if (!isSetupDone) {
      initializeInjector();
      isSetupDone = true;
    }
    // Truncate table before running each test
    dslContext.truncate(DELETED_BLOCKS.getName());
  }

  /**
   * Test method for {@link ScmTableCountTask#processTableCount()}.
   *
   * @throws IOException if an I/O error occurs during the test.
   */
  @Test
  public void testProcessTableCount2() throws IOException {
    addDeletedBlocksData(5);

    // Attempt to fetch the record before running the task as
    // the record does not exist
    for (String tableName : scmTableCountTask.getTaskTables()) {
      assertThrows(
          NullPointerException.class,
          () -> getCountForTable(tableName)
      );
    }

    // Run the task
    scmTableCountTask.processTableCount();

    // Verify that the table counts are updated for each table returned by the
    // getTaskTables
    for (String tableName : scmTableCountTask.getTaskTables()) {
      assertEquals(5, getCountForTable(tableName));
    }
  }


  /**
   * Adds dummy data to the DELETED_BLOCKS table.
   *
   * @throws IOException if an I/O error occurs while adding the data.
   */
  private void addDeletedBlocksData(int totalRecords) throws IOException {
    List<Long> localIdList = Arrays.asList(10L, 20L, 30L, 40L, 50L);

    for (long i = 1; i <= totalRecords; i++) {
      StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction dtx =
          StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction.newBuilder()
              .setTxID(i)
              .setContainerID(100L)
              .addAllLocalID(localIdList)
              .setCount(4)
              .build();

      DELETED_BLOCKS.getTable(this.scmDBStore).put(i, dtx);
    }
  }


  private long getCountForTable(String tableName) {
    String key = TableCountTask.getRowKeyFromTable(tableName);
    return scmTableCountDao.findById(key).getCount();
  }

}

