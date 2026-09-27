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

package org.apache.hadoop.ozone.debug.datanode.container.analyze;

import static org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition.CONTAINERS;

import java.io.File;
import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition;
import org.apache.hadoop.hdds.utils.db.CodecException;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.DBStoreBuilder;
import org.apache.hadoop.hdds.utils.db.RocksDatabaseException;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.cache.TableCache.CacheType;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.common.Storage;
import org.apache.hadoop.ozone.container.common.helpers.DatanodeVersionFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Read-only lookup of container metadata from {@code scm.db}.
 */
public final class ScmContainerMetadataReader implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(ScmContainerMetadataReader.class);
  private final DBStore dbStore;
  private final Table<ContainerID, ContainerInfo> containerTable;

  public ScmContainerMetadataReader(ConfigurationSource conf, File scmDbPath)
      throws IOException {
    File scmDbDir = resolveScmDbDirectory(scmDbPath);
    File parentDir = scmDbDir.getParentFile();
    if (parentDir == null) {
      throw new IOException("SCM database directory has no parent path: " + scmDbDir);
    }
    try {
      this.dbStore = DBStoreBuilder.newBuilder(conf, SCMDBDefinition.get(), scmDbDir.getName(),
          parentDir.toPath())
          .setOpenReadOnly(true)
          .build();
    } catch (RocksDatabaseException e) {
      throw new IOException("Failed to open SCM database at " + scmDbDir, e);
    }
    try {
      this.containerTable = CONTAINERS.getTable(dbStore, CacheType.NO_CACHE);
    } catch (RocksDatabaseException | CodecException e) {
      dbStore.close();
      throw new IOException("Failed to open scm.db containers column family at " + scmDbDir, e);
    }
  }

  /**
   * Classify a container ID against scm.db {@code containers}.
   *
   * @return {@link Optional#empty()} when the container is present in SCM with a
   *     non-DELETED lifecycle state
   */
  public Optional<ScmContainerClassification> classify(long containerId) throws IOException {
    try {
      ContainerInfo info = containerTable.get(ContainerID.valueOf(containerId));
      if (info == null) {
        return Optional.of(ScmContainerClassification.NOT_IN_SCM);
      }
      if (info.isDeleted()) {
        return Optional.of(ScmContainerClassification.DELETED);
      }
      return Optional.empty();
    } catch (RocksDatabaseException | CodecException e) {
      throw new IOException("Failed to read container " + containerId + " from scm.db", e);
    }
  }

  /**
   * Read the cluster ID from the SCM VERSION file adjacent to {@code scmDbDir}.
   *
   * <p>The VERSION file is expected at
   * {@code {scmDbDir.parent}/{@value OzoneConsts#STORAGE_DIR}/current/VERSION}.
   *
   * @return the cluster ID string, or null if the file does not exist or could not be read.
   */
  static String readScmClusterId(File scmDbDir) {
    File parentDir = scmDbDir.getParentFile();
    if (parentDir == null) {
      return null;
    }
    File versionFile = new File(new File(new File(parentDir, OzoneConsts.STORAGE_DIR),
        Storage.STORAGE_DIR_CURRENT), Storage.STORAGE_FILE_VERSION);
    if (!versionFile.exists()) {
      return null;
    }
    try {
      Properties props = DatanodeVersionFile.readFrom(versionFile);
      return props.getProperty(OzoneConsts.CLUSTER_ID);
    } catch (IOException e) {
      LOG.debug("Could not read SCM cluster ID from {}: {}", versionFile, e.getMessage());
      return null;
    }
  }

  static File resolveScmDbDirectory(File path) throws IOException {
    Objects.requireNonNull(path, "scmDbPath");
    File absolutePath = path.getAbsoluteFile();
    File scmDbDir = absolutePath;
    if (!OzoneConsts.SCM_DB_NAME.equals(absolutePath.getName())) {
      File child = new File(absolutePath, OzoneConsts.SCM_DB_NAME);
      if (child.isDirectory()) {
        scmDbDir = child;
      }
    }
    if (!scmDbDir.isDirectory()) {
      throw new IOException("SCM database directory not found: " + path);
    }
    return scmDbDir;
  }

  @Override
  public void close() {
    if (dbStore != null) {
      dbStore.close();
    }
  }

  /**
   * SCM-side classification for an on-disk container directory.
   */
  enum ScmContainerClassification {
    /** No record for this container ID in scm.db {@code containers}. */
    NOT_IN_SCM,
    /** Record exists and {@link ContainerInfo} state is DELETED. */
    DELETED
  }
}
