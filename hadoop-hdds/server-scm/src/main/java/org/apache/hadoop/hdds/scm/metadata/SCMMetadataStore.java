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
package org.apache.hadoop.hdds.scm.metadata;

import java.io.IOException;
import java.math.BigInteger;
import java.security.cert.X509Certificate;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.security.x509.certificate.authority.CertificateStore;
import org.apache.hadoop.hdds.utils.db.BatchOperationHandler;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;

import com.google.common.annotations.VisibleForTesting;

/**
 * Generic interface for data stores for SCM.
 * This is similar to the OMMetadataStore class,
 * where we write classes into some underlying storage system.
 */
public interface SCMMetadataStore {
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
   * A Table that keeps the deleted blocks lists and transactions.
   *
   * @return Table
   */
  Table<Long, DeletedBlocksTransaction> getDeletedBlocksTXTable();

  /**
   * A table that maintains all the valid certificates issued by the SCM CA.
   *
   * @return Table
   */
  Table<BigInteger, X509Certificate> getValidCertsTable();

  /**
   * A Table that maintains all revoked certificates until they expire.
   *
   * @return Table.
   */
  Table<BigInteger, X509Certificate> getRevokedCertsTable();

  /**
   * Returns the list of Certificates of a specific type.
   *
   * @param certType - CertType.
   * @return Iterator<X509Certificate>
   */
  TableIterator getAllCerts(CertificateStore.CertType certType);

  /**
   * A Table that maintains all the pipeline information.
   */
  Table<PipelineID, Pipeline> getPipelineTable();

  /**
   * Helper to create and write batch transactions.
   */
  BatchOperationHandler getBatchHandler();

  /**
   * Table that maintains all the container information.
   */
  Table<ContainerID, ContainerInfo> getContainerTable();
}
