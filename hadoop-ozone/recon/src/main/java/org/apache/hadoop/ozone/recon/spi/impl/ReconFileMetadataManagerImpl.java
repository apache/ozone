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

import static org.apache.hadoop.ozone.recon.spi.impl.ReconDBDefinition.FILE_COUNT_BY_SIZE;
import static org.apache.hadoop.ozone.recon.spi.impl.ReconDBProvider.truncateTable;

import java.io.IOException;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.RDBBatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.recon.spi.ReconFileMetadataManager;
import org.apache.hadoop.ozone.recon.tasks.FileSizeCountKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the Recon File Metadata DB Service.
 */
@Singleton
public class ReconFileMetadataManagerImpl implements ReconFileMetadataManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(ReconFileMetadataManagerImpl.class);

  private Table<FileSizeCountKey, Long> fileCountTable;
  private DBStore fileMetadataDbStore;

  @Inject
  public ReconFileMetadataManagerImpl(ReconDBProvider reconDBProvider) {
    this(reconDBProvider.getDbStore());
  }

  private ReconFileMetadataManagerImpl(DBStore reconDBStore) {
    fileMetadataDbStore = reconDBStore;
    initializeTables();
  }

  @Override
  public ReconFileMetadataManager getStagedReconFileMetadataManager(
      DBStore stagedReconDbStore) {
    return new ReconFileMetadataManagerImpl(stagedReconDbStore);
  }

  @Override
  public void reinitialize(ReconDBProvider reconDBProvider) {
    fileMetadataDbStore = reconDBProvider.getDbStore();
    initializeTables();
  }

  /**
   * Initialize the file metadata DB tables.
   */
  private void initializeTables() {
    try {
      this.fileCountTable = FILE_COUNT_BY_SIZE.getTable(fileMetadataDbStore);
    } catch (IOException e) {
      LOG.error("Unable to create File Size Count table.", e);
    }
  }

  @Override
  public void batchStoreFileSizeCount(BatchOperation batch,
                                      FileSizeCountKey fileSizeCountKey,
                                      Long count) throws IOException {
    fileCountTable.putWithBatch(batch, fileSizeCountKey, count);
  }

  @Override
  public void batchDeleteFileSizeCount(BatchOperation batch,
                                       FileSizeCountKey fileSizeCountKey) throws IOException {
    fileCountTable.deleteWithBatch(batch, fileSizeCountKey);
  }

  @Override
  public Long getFileSizeCount(FileSizeCountKey fileSizeCountKey) throws IOException {
    return fileCountTable.get(fileSizeCountKey);
  }

  @Override
  public Table<FileSizeCountKey, Long> getFileCountTable() {
    return fileCountTable;
  }

  @Override
  public void commitBatchOperation(RDBBatchOperation rdbBatchOperation)
      throws IOException {
    fileMetadataDbStore.commitBatchOperation(rdbBatchOperation);
  }

  @Override
  public void clearFileCountTable() throws IOException {
    truncateTable(fileCountTable);
    LOG.info("Successfully cleared file count table");
  }
}
