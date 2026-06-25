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

package org.apache.hadoop.ozone.recon.spi;

import java.io.IOException;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.RDBBatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.recon.spi.impl.ReconDBProvider;
import org.apache.hadoop.ozone.recon.tasks.FileSizeCountKey;

/**
 * The Recon File Metadata DB Service interface for file size counts.
 */
@InterfaceStability.Unstable
public interface ReconFileMetadataManager {

  /**
   * Returns staged DB file metadata manager.
   *
   * @param stagedReconDbStore staged Recon DB store
   * @return ReconFileMetadataManager
   */
  ReconFileMetadataManager getStagedReconFileMetadataManager(DBStore stagedReconDbStore);

  /**
   * reinitialize the ReconFileMetadataManager.
   *
   * @param reconDBProvider recon DB provider to reinitialize with.
   */
  void reinitialize(ReconDBProvider reconDBProvider);

  /**
   * Store the file size count mapping into a batch.
   *
   * @param batch the batch operation we store into
   * @param fileSizeCountKey the file size count key.
   * @param count              Count of files with that size range.
   */
  void batchStoreFileSizeCount(BatchOperation batch,
                               FileSizeCountKey fileSizeCountKey,
                               Long count) throws IOException;

  /**
   * Delete file size count mapping from a batch.
   *
   * @param batch the batch operation we add the deletion to
   * @param fileSizeCountKey the file size count key to be deleted.
   */
  void batchDeleteFileSizeCount(BatchOperation batch,
                                FileSizeCountKey fileSizeCountKey) throws IOException;

  /**
   * Get the stored file size count for the given key.
   *
   * @param fileSizeCountKey the file size count key.
   * @return count of files with that size range.
   */
  Long getFileSizeCount(FileSizeCountKey fileSizeCountKey) throws IOException;

  /**
   * Get the entire fileCountTable.
   * @return fileCountTable
   */
  Table<FileSizeCountKey, Long> getFileCountTable();

  /**
   * Commit a batch operation into the fileMetadataDbStore.
   *
   * @param rdbBatchOperation batch operation we want to commit
   */
  void commitBatchOperation(RDBBatchOperation rdbBatchOperation)
      throws IOException;

  /**
   * Clear all file size count data from the table.
   * This method is used during reprocess operations.
   */
  void clearFileCountTable() throws IOException;
}
