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

package org.apache.hadoop.hdds.scm.pipeline;

import static org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition.PIPELINES;

import java.io.IOException;
import java.math.BigInteger;
import java.security.cert.X509Certificate;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.metadata.PipelineCodec;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStore;
import org.apache.hadoop.hdds.security.x509.certificate.authority.CertificateStore;
import org.apache.hadoop.hdds.utils.db.BatchOperationHandler;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.DBColumnFamilyDefinition;
import org.apache.hadoop.hdds.utils.db.DBDefinition;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;

/**
 * Test SCM Metadata Store that has ONLY the pipeline table whose key uses the
 * old codec format.
 */
public class TestSCMStoreImplWithOldPipelineIDKeyFormat
    implements SCMMetadataStore {

  private DBStore store;
  private final OzoneConfiguration configuration;
  private Table<PipelineID, Pipeline> pipelineTable;

  public TestSCMStoreImplWithOldPipelineIDKeyFormat(
      OzoneConfiguration config) throws IOException {
    this.configuration = config;
    start(configuration);
  }

  @Override
  public void start(OzoneConfiguration config)
      throws IOException {
    if (this.store == null) {
      this.store = DBStoreBuilder.createDBStore(config,
          new SCMDBTestDefinition());
      pipelineTable = PIPELINES.getTable(store);
    }
  }

  @Override
  public void stop() throws Exception {
    if (store != null) {
      store.close();
      store = null;
    }
  }

  @Override
  public DBStore getStore() {
    return null;
  }

  @Override
  public Table<Long, DeletedBlocksTransaction> getDeletedBlocksTXTable() {
    return null;
  }

  @Override
  public Table<BigInteger, X509Certificate> getValidCertsTable() {
    return null;
  }

  @Override
  public Table<BigInteger, X509Certificate> getRevokedCertsTable() {
    return null;
  }

  @Override
  public TableIterator getAllCerts(CertificateStore.CertType certType) {
    return null;
  }

  @Override
  public Table<PipelineID, Pipeline> getPipelineTable() {
    return pipelineTable;
  }

  @Override
  public BatchOperationHandler getBatchHandler() {
    return null;
  }

  @Override
  public Table<ContainerID, ContainerInfo> getContainerTable() {
    return null;
  }

  /**
   * Test SCM DB Definition for the above class.
   */
  public static class SCMDBTestDefinition implements DBDefinition {

    public static final DBColumnFamilyDefinition<PipelineID, Pipeline>
        PIPELINES =
        new DBColumnFamilyDefinition<>(
            "pipelines",
            PipelineID.class,
            new OldPipelineIDCodec(),
            Pipeline.class,
            new PipelineCodec());

    @Override
    public String getName() {
      return "scm.db";
    }

    @Override
    public String getLocationConfigKey() {
      return ScmConfigKeys.OZONE_SCM_DB_DIRS;
    }

    @Override
    public DBColumnFamilyDefinition[] getColumnFamilies() {
      return new DBColumnFamilyDefinition[] {PIPELINES};
    }
  }

  /**
   * Old Pipeline ID codec that relies on protobuf serialization.
   */
  public static class OldPipelineIDCodec implements Codec<PipelineID> {
    @Override
    public byte[] toPersistedFormat(PipelineID object) throws IOException {
      return object.getProtobuf().toByteArray();
    }

    @Override
    public PipelineID fromPersistedFormat(byte[] rawData) throws IOException {
      return null;
    }

    @Override
    public PipelineID copyObject(PipelineID object) {
      throw new UnsupportedOperationException();
    }
  }

}

