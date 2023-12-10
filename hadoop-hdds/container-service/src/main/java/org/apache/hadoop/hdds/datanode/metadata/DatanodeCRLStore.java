/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.datanode.metadata;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.security.x509.crl.CRLInfo;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.Table;

import java.io.IOException;
import java.util.List;

/**
 * Generic interface for data stores for Datanode.
 * This is similar to the OMMetadataStore class,
 * where we write classes into some underlying storage system.
 */
public interface DatanodeCRLStore {

  /**
   * Start metadata manager.
   *
   * @param configuration - Configuration
   * @throws IOException - Unable to start metadata store.
   */
  void start(OzoneConfiguration configuration) throws IOException;

  /**
   * Stop metadata manager.
   */
  void stop() throws Exception;

  /**
   * Get metadata store.
   *
   * @return metadata store.
   */
  @VisibleForTesting
  DBStore getStore();

  /**
   * A table to store the latest processed CRL sequence Id.
   * @return Table
   */
  Table<String, Long> getCRLSequenceIdTable();

  /**
   * A table to store all the pending CRLs for future revocation of
   * certificates.
   * @return Table
   */
  Table<Long, CRLInfo> getPendingCRLsTable();

  /**
   * Returns the latest processed CRL Sequence ID.
   * @return CRL Sequence ID.
   * @throws IOException on error.
   */
  Long getLatestCRLSequenceID() throws IOException;

  /**
   * Return a list of CRLs that are pending revocation.
   * @return a list of CRLInfo.
   * @throws IOException on error.
   */
  List<CRLInfo> getPendingCRLs() throws IOException;

}
