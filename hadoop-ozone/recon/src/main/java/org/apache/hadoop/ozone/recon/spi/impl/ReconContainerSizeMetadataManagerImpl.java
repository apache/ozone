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

package org.apache.hadoop.ozone.recon.spi.impl;

import static org.apache.hadoop.ozone.recon.spi.impl.ReconDBDefinition.CONTAINER_COUNT_BY_SIZE;
import static org.apache.hadoop.ozone.recon.spi.impl.ReconDBProvider.truncateTable;

import java.io.IOException;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.RDBBatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.recon.spi.ReconContainerSizeMetadataManager;
import org.apache.hadoop.ozone.recon.tasks.ContainerSizeCountKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the Recon Container Size Metadata DB Service.
 */
@Singleton
public class ReconContainerSizeMetadataManagerImpl
    implements ReconContainerSizeMetadataManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(ReconContainerSizeMetadataManagerImpl.class);

  private Table<ContainerSizeCountKey, Long> containerCountTable;
  private DBStore containerSizeMetadataDbStore;

  @Inject
  public ReconContainerSizeMetadataManagerImpl(ReconDBProvider reconDBProvider) {
    this(reconDBProvider.getDbStore());
  }

  private ReconContainerSizeMetadataManagerImpl(DBStore reconDBStore) {
    containerSizeMetadataDbStore = reconDBStore;
    initializeTables();
  }

  @Override
  public ReconContainerSizeMetadataManager getStagedReconContainerSizeMetadataManager(
      DBStore stagedReconDbStore) {
    return new ReconContainerSizeMetadataManagerImpl(stagedReconDbStore);
  }

  @Override
  public void reinitialize(ReconDBProvider reconDBProvider) {
    containerSizeMetadataDbStore = reconDBProvider.getDbStore();
    initializeTables();
  }

  /**
   * Initialize the container size metadata DB tables.
   */
  private void initializeTables() {
    try {
      this.containerCountTable = CONTAINER_COUNT_BY_SIZE.getTable(containerSizeMetadataDbStore);
    } catch (IOException e) {
      LOG.error("Unable to create Container Size Count table.", e);
    }
  }

  @Override
  public void batchStoreContainerSizeCount(BatchOperation batch,
                                           ContainerSizeCountKey containerSizeCountKey,
                                           Long count) throws IOException {
    containerCountTable.putWithBatch(batch, containerSizeCountKey, count);
  }

  @Override
  public void batchDeleteContainerSizeCount(BatchOperation batch,
                                            ContainerSizeCountKey containerSizeCountKey)
      throws IOException {
    containerCountTable.deleteWithBatch(batch, containerSizeCountKey);
  }

  @Override
  public Long getContainerSizeCount(ContainerSizeCountKey containerSizeCountKey)
      throws IOException {
    return containerCountTable.get(containerSizeCountKey);
  }

  @Override
  public Table<ContainerSizeCountKey, Long> getContainerCountTable() {
    return containerCountTable;
  }

  @Override
  public void commitBatchOperation(RDBBatchOperation rdbBatchOperation)
      throws IOException {
    containerSizeMetadataDbStore.commitBatchOperation(rdbBatchOperation);
  }

  @Override
  public void clearContainerCountTable() throws IOException {
    truncateTable(containerCountTable);
    LOG.info("Successfully cleared container count table");
  }
}
