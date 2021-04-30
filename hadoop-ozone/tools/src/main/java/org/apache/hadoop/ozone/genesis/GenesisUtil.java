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
package org.apache.hadoop.ozone.genesis;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStore;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStoreImpl;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.server.SCMConfigurator;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.utils.MetadataStore;
import org.apache.hadoop.hdds.utils.MetadataStoreBuilder;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.common.Storage;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.security.authentication.client.AuthenticationException;

import org.apache.commons.lang3.RandomStringUtils;


/**
 * Utility class for benchmark test cases.
 */
public final class GenesisUtil {

  private GenesisUtil() {
    // private constructor.
  }

  public static final String DEFAULT_TYPE = "default";
  public static final String CACHE_10MB_TYPE = "Cache10MB";
  public static final String CACHE_1GB_TYPE = "Cache1GB";
  public static final String CLOSED_TYPE = "ClosedContainer";

  private static final int DB_FILE_LEN = 7;
  private static final String TMP_DIR = "java.io.tmpdir";
  private static final Random RANDOM = new Random();
  private static final String RANDOM_LOCAL_ADDRESS = "127.0.0.1:0";

  public static Path getTempPath() {
    return Paths.get(System.getProperty(TMP_DIR));
  }

  public static MetadataStore getMetadataStore(String dbType)
      throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    MetadataStoreBuilder builder = MetadataStoreBuilder.newBuilder();
    builder.setConf(conf);
    builder.setCreateIfMissing(true);
    builder.setDbFile(
        getTempPath().resolve(RandomStringUtils.randomNumeric(DB_FILE_LEN))
            .toFile());
    switch (dbType) {
    case DEFAULT_TYPE:
      break;
    case CLOSED_TYPE:
      break;
    case CACHE_10MB_TYPE:
      builder.setCacheSize((long) StorageUnit.MB.toBytes(10));
      break;
    case CACHE_1GB_TYPE:
      builder.setCacheSize((long) StorageUnit.GB.toBytes(1));
      break;
    default:
      throw new IllegalStateException("Unknown type: " + dbType);
    }
    return builder.build();
  }

  public static DatanodeDetails createDatanodeDetails(UUID uuid) {
    String ipAddress =
        RANDOM.nextInt(256) + "." + RANDOM.nextInt(256) + "." + RANDOM
            .nextInt(256) + "." + RANDOM.nextInt(256);

    DatanodeDetails.Port containerPort = DatanodeDetails.newPort(
        DatanodeDetails.Port.Name.STANDALONE, 0);
    DatanodeDetails.Port ratisPort = DatanodeDetails.newPort(
        DatanodeDetails.Port.Name.RATIS, 0);
    DatanodeDetails.Port restPort = DatanodeDetails.newPort(
        DatanodeDetails.Port.Name.REST, 0);
    DatanodeDetails.Builder builder = DatanodeDetails.newBuilder();
    builder.setUuid(uuid)
        .setHostName("localhost")
        .setIpAddress(ipAddress)
        .addPort(containerPort)
        .addPort(ratisPort)
        .addPort(restPort);
    return builder.build();
  }

  static StorageContainerManager getScm(OzoneConfiguration conf,
      SCMConfigurator configurator) throws IOException,
      AuthenticationException {
    SCMStorageConfig scmStore = new SCMStorageConfig(conf);
    if(scmStore.getState() != Storage.StorageState.INITIALIZED) {
      String clusterId = UUID.randomUUID().toString();
      String scmId = UUID.randomUUID().toString();
      scmStore.setClusterId(clusterId);
      scmStore.setScmId(scmId);
      // writes the version file properties
      scmStore.initialize();
    }
    return StorageContainerManager.createSCM(conf, configurator);
  }

  static void configureSCM(OzoneConfiguration conf, int numHandlers) {
    conf.set(ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY,
        RANDOM_LOCAL_ADDRESS);
    conf.set(ScmConfigKeys.OZONE_SCM_BLOCK_CLIENT_ADDRESS_KEY,
        RANDOM_LOCAL_ADDRESS);
    conf.set(ScmConfigKeys.OZONE_SCM_DATANODE_ADDRESS_KEY,
        RANDOM_LOCAL_ADDRESS);
    conf.set(ScmConfigKeys.OZONE_SCM_HTTP_ADDRESS_KEY,
        RANDOM_LOCAL_ADDRESS);
    conf.setInt(ScmConfigKeys.OZONE_SCM_HANDLER_COUNT_KEY, numHandlers);
  }

  static void addPipelines(HddsProtos.ReplicationFactor factor,
      int numPipelines, ConfigurationSource conf) throws Exception {
    SCMMetadataStore scmMetadataStore =
            new SCMMetadataStoreImpl((OzoneConfiguration)conf);

    Table<PipelineID, Pipeline> pipelineTable =
        scmMetadataStore.getPipelineTable();
    List<DatanodeDetails> nodes = new ArrayList<>();
    for (int i = 0; i < factor.getNumber(); i++) {
      nodes
          .add(GenesisUtil.createDatanodeDetails(UUID.randomUUID()));
    }
    for (int i = 0; i < numPipelines; i++) {
      Pipeline pipeline =
          Pipeline.newBuilder()
              .setState(Pipeline.PipelineState.OPEN)
              .setId(PipelineID.randomId())
              .setReplicationConfig(new RatisReplicationConfig(factor))
              .setNodes(nodes)
              .build();
      pipelineTable.put(pipeline.getId(),
          pipeline);
    }
    scmMetadataStore.getStore().close();
  }

  static OzoneManager getOm(OzoneConfiguration conf)
      throws IOException, AuthenticationException {
    OMStorage omStorage = new OMStorage(conf);
    SCMStorageConfig scmStore = new SCMStorageConfig(conf);
    if (omStorage.getState() != Storage.StorageState.INITIALIZED) {
      omStorage.setClusterId(scmStore.getClusterID());
      omStorage.setOmId(UUID.randomUUID().toString());
      omStorage.initialize();
    }
    return OzoneManager.createOm(conf);
  }

  static void configureOM(OzoneConfiguration conf, int numHandlers) {
    conf.set(OMConfigKeys.OZONE_OM_HTTP_ADDRESS_KEY,
        RANDOM_LOCAL_ADDRESS);
    conf.setInt(OMConfigKeys.OZONE_OM_HANDLER_COUNT_KEY, numHandlers);
  }
}
