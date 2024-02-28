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
package org.apache.hadoop.ozone;

import org.apache.hadoop.hdds.conf.ConfigurationTarget;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.container.common.DatanodeLayoutStorage;
import org.apache.hadoop.ozone.container.replication.ReplicationServer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.hdds.DFSConfigKeysLegacy.DFS_DATANODE_DATA_DIR_KEY;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DATANODE_CLIENT_ADDRESS_KEY;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_DATANODE_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_DATANODE_DIR_DU_RESERVED;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_DATANODE_DIR_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_REST_HTTP_ADDRESS_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.DFS_CONTAINER_IPC_PORT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.DFS_CONTAINER_RATIS_ADMIN_PORT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.DFS_CONTAINER_RATIS_DATANODE_STORAGE_DIR;
import static org.apache.hadoop.ozone.OzoneConfigKeys.DFS_CONTAINER_RATIS_DATASTREAM_PORT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.DFS_CONTAINER_RATIS_IPC_PORT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.DFS_CONTAINER_RATIS_SERVER_PORT;
import static org.apache.ozone.test.GenericTestUtils.PortAllocator.anyHostWithFreePort;
import static org.apache.ozone.test.GenericTestUtils.PortAllocator.getFreePort;

/**
 * Creates datanodes with similar configuration (same number of volumes, same layout version, etc.).
 */
public class UniformDatanodesFactory implements MiniOzoneCluster.DatanodeFactory {

  private final AtomicInteger nodesCreated = new AtomicInteger();

  private final int numDataVolumes;
  private final String reservedSpace;
  private final Integer layoutVersion;

  protected UniformDatanodesFactory(Builder builder) {
    numDataVolumes = builder.numDataVolumes;
    layoutVersion = builder.layoutVersion;
    reservedSpace = builder.reservedSpace;
  }

  @Override
  public OzoneConfiguration apply(OzoneConfiguration conf) throws IOException {
    final int i = nodesCreated.incrementAndGet();
    final OzoneConfiguration dnConf = new OzoneConfiguration(conf);

    configureDatanodePorts(dnConf);

    Path baseDir = Paths.get(Objects.requireNonNull(conf.get(OZONE_METADATA_DIRS)), "datanode-" + i);

    Path metaDir = baseDir.resolve("meta");
    Files.createDirectories(metaDir);
    dnConf.set(OZONE_METADATA_DIRS, metaDir.toString());

    List<String> dataDirs = new ArrayList<>();
    List<String> reservedSpaceList = new ArrayList<>();
    for (int j = 0; j < numDataVolumes; j++) {
      Path dir = baseDir.resolve("data-" + j);
      Files.createDirectories(dir);
      dataDirs.add(dir.toString());
      if (reservedSpace != null) {
        reservedSpaceList.add(dir + ":" + reservedSpace);
      }
    }
    String reservedSpaceString = String.join(",", reservedSpaceList);
    String listOfDirs = String.join(",", dataDirs);
    dnConf.set(DFS_DATANODE_DATA_DIR_KEY, listOfDirs);
    dnConf.set(HDDS_DATANODE_DIR_KEY, listOfDirs);
    dnConf.set(HDDS_DATANODE_DIR_DU_RESERVED, reservedSpaceString);

    Path ratisDir = baseDir.resolve("ratis");
    Files.createDirectories(ratisDir);
    dnConf.set(DFS_CONTAINER_RATIS_DATANODE_STORAGE_DIR, ratisDir.toString());

    if (layoutVersion != null) {
      DatanodeLayoutStorage layoutStorage = new DatanodeLayoutStorage(
          dnConf, UUID.randomUUID().toString(), layoutVersion);
      layoutStorage.initialize();
    }

    return dnConf;
  }

  private void configureDatanodePorts(ConfigurationTarget conf) {
    conf.set(HDDS_REST_HTTP_ADDRESS_KEY, anyHostWithFreePort());
    conf.set(HDDS_DATANODE_HTTP_ADDRESS_KEY, anyHostWithFreePort());
    conf.set(HDDS_DATANODE_CLIENT_ADDRESS_KEY, anyHostWithFreePort());
    conf.setInt(DFS_CONTAINER_IPC_PORT, getFreePort());
    conf.setInt(DFS_CONTAINER_RATIS_IPC_PORT, getFreePort());
    conf.setInt(DFS_CONTAINER_RATIS_ADMIN_PORT, getFreePort());
    conf.setInt(DFS_CONTAINER_RATIS_SERVER_PORT, getFreePort());
    conf.setInt(DFS_CONTAINER_RATIS_DATASTREAM_PORT, getFreePort());
    conf.setFromObject(new ReplicationServer.ReplicationConfig().setPort(getFreePort()));
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder for UniformDatanodesFactory.
   */
  public static class Builder {

    private int numDataVolumes = 1;
    private String reservedSpace;
    private Integer layoutVersion;

    /**
     * Sets the number of data volumes per datanode.
     */
    public Builder setNumDataVolumes(int n) {
      numDataVolumes = n;
      return this;
    }

    /**
     * Sets the reserved space
     * {@link org.apache.hadoop.hdds.scm.ScmConfigKeys#HDDS_DATANODE_DIR_DU_RESERVED}
     * for each volume in each datanode.
     * @param reservedSpace String that contains the numeric size value and ends with a
     *   {@link org.apache.hadoop.hdds.conf.StorageUnit} suffix. For example, "50GB".
     * @see org.apache.hadoop.ozone.container.common.volume.VolumeInfo
     */
    public Builder setReservedSpace(String reservedSpace) {
      this.reservedSpace = reservedSpace;
      return this;
    }

    public Builder setLayoutVersion(int layoutVersion) {
      this.layoutVersion = layoutVersion;
      return this;
    }

    public UniformDatanodesFactory build() {
      return new UniformDatanodesFactory(this);
    }

  }

}
