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

package org.apache.hadoop.ozone.om;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.hadoop.hdds.server.YamlUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.ozone.compaction.log.SstFileInfo;
import org.apache.ratis.util.function.UncheckedAutoCloseableSupplier;
import org.rocksdb.LiveFileMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.AbstractConstruct;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import org.yaml.snakeyaml.error.YAMLException;
import org.yaml.snakeyaml.introspector.BeanAccess;
import org.yaml.snakeyaml.introspector.Property;
import org.yaml.snakeyaml.introspector.PropertyUtils;
import org.yaml.snakeyaml.nodes.MappingNode;
import org.yaml.snakeyaml.nodes.Node;
import org.yaml.snakeyaml.nodes.NodeTuple;
import org.yaml.snakeyaml.nodes.ScalarNode;
import org.yaml.snakeyaml.nodes.Tag;
import org.yaml.snakeyaml.representer.Represent;
import org.yaml.snakeyaml.representer.Representer;

/**
 * Class for creating and reading snapshot local properties / data YAML files.
 * Checksum of the YAML fields are computed and stored in the YAML file transparently to callers.
 * Inspired by org.apache.hadoop.ozone.container.common.impl.ContainerDataYaml
 */
public final class OmSnapshotLocalDataYaml extends OmSnapshotLocalData {

  private static final Logger LOG = LoggerFactory.getLogger(OmSnapshotLocalDataYaml.class);

  public static final Tag SNAPSHOT_YAML_TAG = new Tag("OmSnapshotLocalData");
  public static final Tag SNAPSHOT_VERSION_META_TAG = new Tag("VersionMeta");
  public static final Tag SST_FILE_INFO_TAG = new Tag("SstFileInfo");

  /**
   * Creates a new OmSnapshotLocalDataYaml with default values.
   */
  public OmSnapshotLocalDataYaml(List<LiveFileMetaData> liveFileMetaDatas, UUID previousSnapshotId) {
    super(liveFileMetaDatas, previousSnapshotId);
  }

  /**
   * Copy constructor to create a deep copy.
   * @param source The source OmSnapshotLocalData to copy from
   */
  public OmSnapshotLocalDataYaml(OmSnapshotLocalData source) {
    super(source);
  }

  /**
   * Verifies the checksum of the snapshot data.
   * @param snapshotData The snapshot data to verify
   * @return true if the checksum is valid, false otherwise
   * @throws IOException if there's an error computing the checksum
   */
  public static boolean verifyChecksum(OmSnapshotManager snapshotManager, OmSnapshotLocalData snapshotData)
      throws IOException {
    Preconditions.checkNotNull(snapshotData, "snapshotData cannot be null");

    // Get the stored checksum
    String storedChecksum = snapshotData.getChecksum();
    if (storedChecksum == null) {
      LOG.warn("No checksum found in snapshot data for verification");
      return false;
    }

    // Create a copy of the snapshot data for computing checksum
    OmSnapshotLocalDataYaml snapshotDataCopy = new OmSnapshotLocalDataYaml(snapshotData);

    // Clear the existing checksum in the copy
    snapshotDataCopy.setChecksum(null);

    // Get the YAML representation
    try (UncheckedAutoCloseableSupplier<Yaml> yaml = snapshotManager.getSnapshotLocalYaml()) {
      // Compute new checksum
      snapshotDataCopy.computeAndSetChecksum(yaml.get());

      // Compare the stored and computed checksums
      String computedChecksum = snapshotDataCopy.getChecksum();
      boolean isValid = storedChecksum.equals(computedChecksum);

      if (!isValid) {
        LOG.warn("Checksum verification failed for snapshot local data. " +
            "Stored: {}, Computed: {}", storedChecksum, computedChecksum);
      }
      return isValid;
    }
  }

  /**
   * Representer class to define which fields need to be stored in yaml file.
   */
  private static class OmSnapshotLocalDataRepresenter extends Representer {

    OmSnapshotLocalDataRepresenter(DumperOptions options) {
      super(options);
      this.addClassTag(OmSnapshotLocalDataYaml.class, SNAPSHOT_YAML_TAG);
      this.addClassTag(VersionMeta.class, SNAPSHOT_VERSION_META_TAG);
      this.addClassTag(SstFileInfo.class, SST_FILE_INFO_TAG);
      representers.put(SstFileInfo.class, new RepresentSstFileInfo());
      representers.put(VersionMeta.class, new RepresentVersionMeta());
      representers.put(UUID.class, data ->
          new ScalarNode(Tag.STR, data.toString(), null, null, DumperOptions.ScalarStyle.PLAIN));
    }

    private class RepresentSstFileInfo implements Represent {
      @Override
      public Node representData(Object data) {
        SstFileInfo info = (SstFileInfo) data;
        Map<String, Object> map = new java.util.LinkedHashMap<>();
        map.put(OzoneConsts.OM_SST_FILE_INFO_FILE_NAME, info.getFileName());
        map.put(OzoneConsts.OM_SST_FILE_INFO_START_KEY, info.getStartKey());
        map.put(OzoneConsts.OM_SST_FILE_INFO_END_KEY, info.getEndKey());
        map.put(OzoneConsts.OM_SST_FILE_INFO_COL_FAMILY, info.getColumnFamily());

        // Explicitly create a mapping node with the desired tag
        return representMapping(SST_FILE_INFO_TAG, map, DumperOptions.FlowStyle.BLOCK);
      }
    }

    // New inner class for VersionMeta
    private class RepresentVersionMeta implements Represent {
      @Override
      public Node representData(Object data) {
        VersionMeta meta = (VersionMeta) data;
        Map<String, Object> map = new java.util.LinkedHashMap<>();
        map.put(OzoneConsts.OM_SLD_VERSION_META_PREV_SNAP_VERSION, meta.getPreviousSnapshotVersion());
        map.put(OzoneConsts.OM_SLD_VERSION_META_SST_FILES, meta.getSstFiles());

        return representMapping(SNAPSHOT_VERSION_META_TAG, map, DumperOptions.FlowStyle.BLOCK);
      }
    }


    /**
     * Omit properties with null value.
     */
    @Override
    protected NodeTuple representJavaBeanProperty(
        Object bean, Property property, Object value, Tag tag) {
      return value == null
          ? null
          : super.representJavaBeanProperty(bean, property, value, tag);
    }
  }

  /**
   * Constructor class for OmSnapshotLocalData.
   * This is used when parsing YAML files into OmSnapshotLocalDataYaml objects.
   */
  private static class SnapshotLocalDataConstructor extends SafeConstructor {
    SnapshotLocalDataConstructor() {
      super(new LoaderOptions());
      //Adding our own specific constructors for tags.
      this.yamlConstructors.put(SNAPSHOT_YAML_TAG, new ConstructSnapshotLocalData());
      this.yamlConstructors.put(SNAPSHOT_VERSION_META_TAG, new ConstructVersionMeta());
      this.yamlConstructors.put(SST_FILE_INFO_TAG, new ConstructSstFileInfo());
      TypeDescription omDesc = new TypeDescription(OmSnapshotLocalDataYaml.class);
      omDesc.putMapPropertyType(OzoneConsts.OM_SLD_VERSION_SST_FILE_INFO, Integer.class, VersionMeta.class);
      this.addTypeDescription(omDesc);
      TypeDescription versionMetaDesc = new TypeDescription(VersionMeta.class);
      versionMetaDesc.putListPropertyType(OzoneConsts.OM_SLD_VERSION_META_SST_FILES, SstFileInfo.class);
      this.addTypeDescription(versionMetaDesc);
    }

    private final class ConstructSstFileInfo extends AbstractConstruct {
      @Override
      public Object construct(Node node) {
        MappingNode mnode = (MappingNode) node;
        Map<Object, Object> nodes = constructMapping(mnode);
        return new SstFileInfo((String) nodes.get(OzoneConsts.OM_SST_FILE_INFO_FILE_NAME),
            (String) nodes.get(OzoneConsts.OM_SST_FILE_INFO_START_KEY),
            (String) nodes.get(OzoneConsts.OM_SST_FILE_INFO_END_KEY),
            (String) nodes.get(OzoneConsts.OM_SST_FILE_INFO_COL_FAMILY));
      }
    }

    private final class ConstructVersionMeta extends AbstractConstruct {

      @Override
      public Object construct(Node node) {
        MappingNode mnode = (MappingNode) node;
        Map<Object, Object> nodes = constructMapping(mnode);
        return new VersionMeta((Integer) nodes.get(OzoneConsts.OM_SLD_VERSION_META_PREV_SNAP_VERSION),
            (List<SstFileInfo>) nodes.get(OzoneConsts.OM_SLD_VERSION_META_SST_FILES));
      }
    }

    private final class ConstructSnapshotLocalData extends AbstractConstruct {
      @SuppressWarnings("unchecked")
      @Override
      public Object construct(Node node) {
        MappingNode mnode = (MappingNode) node;
        Map<Object, Object> nodes = constructMapping(mnode);
        UUID prevSnapId = UUID.fromString((String) nodes.get(OzoneConsts.OM_SLD_PREV_SNAP_ID));
        OmSnapshotLocalDataYaml snapshotLocalData = new OmSnapshotLocalDataYaml(Collections.emptyList(), prevSnapId);

        // Set version from YAML
        Integer version = (Integer) nodes.get(OzoneConsts.OM_SLD_VERSION);
        snapshotLocalData.setVersion(version);

        // Set other fields from parsed YAML
        snapshotLocalData.setSstFiltered((Boolean) nodes.getOrDefault(OzoneConsts.OM_SLD_IS_SST_FILTERED, false));

        // Handle potential Integer/Long type mismatch from YAML parsing
        Object lastCompactionTimeObj = nodes.getOrDefault(OzoneConsts.OM_SLD_LAST_COMPACTION_TIME, -1L);
        long lastCompactionTime;
        if (lastCompactionTimeObj instanceof Number) {
          lastCompactionTime = ((Number) lastCompactionTimeObj).longValue();
        } else {
          throw new IllegalArgumentException("Invalid type for lastCompactionTime: " +
              lastCompactionTimeObj.getClass().getName() + ". Expected Number type.");
        }
        snapshotLocalData.setLastCompactionTime(lastCompactionTime);

        snapshotLocalData.setNeedsCompaction((Boolean) nodes.getOrDefault(OzoneConsts.OM_SLD_NEEDS_COMPACTION, false));
        Map<Integer, VersionMeta> versionMetaMap =
            (Map<Integer, VersionMeta>) nodes.get(OzoneConsts.OM_SLD_VERSION_SST_FILE_INFO);
        if (versionMetaMap != null) {
          snapshotLocalData.setVersionSstFileInfos(versionMetaMap);
        }

        String checksum = (String) nodes.get(OzoneConsts.OM_SLD_CHECKSUM);
        if (checksum != null) {
          snapshotLocalData.setChecksum(checksum);
        }

        return snapshotLocalData;
      }
    }
  }

  /**
   * Returns the YAML representation of this object as a String
   * (without triggering checksum computation or persistence).
   * @return YAML string representation
   */
  public String getYaml(OmSnapshotManager snapshotManager) throws IOException {
    try (UncheckedAutoCloseableSupplier<Yaml> yaml = snapshotManager.getSnapshotLocalYaml()) {
      return yaml.get().dump(this);
    }
  }

  /**
   * Computes checksum (stored in this object), and writes this object to a YAML file.
   * @param yamlFile The file to write to
   * @throws IOException If there's an error writing to the file
   */
  public void writeToYaml(OmSnapshotManager snapshotManager, File yamlFile) throws IOException {
    // Create Yaml
    try (UncheckedAutoCloseableSupplier<Yaml> yaml = snapshotManager.getSnapshotLocalYaml()) {
      // Compute Checksum and update SnapshotData
      computeAndSetChecksum(yaml.get());
      // Write the SnapshotData with checksum to Yaml file.
      YamlUtils.dump(yaml.get(), this, yamlFile, LOG);
    }
  }

  /**
   * Creates a OmSnapshotLocalDataYaml instance from a YAML file.
   * @param yamlFile The YAML file to read from
   * @return A new OmSnapshotLocalDataYaml instance
   * @throws IOException If there's an error reading the file
   */
  public static OmSnapshotLocalDataYaml getFromYamlFile(OmSnapshotManager snapshotManager, File yamlFile)
      throws IOException {
    Preconditions.checkNotNull(yamlFile, "yamlFile cannot be null");
    try (InputStream inputFileStream = Files.newInputStream(yamlFile.toPath())) {
      return getFromYamlStream(snapshotManager, inputFileStream);
    }
  }

  /**
   * Read the YAML content InputStream, and return OmSnapshotLocalDataYaml instance.
   * @throws IOException
   */
  public static OmSnapshotLocalDataYaml getFromYamlStream(OmSnapshotManager snapshotManager,
      InputStream input) throws IOException {
    OmSnapshotLocalDataYaml dataYaml;
    try (UncheckedAutoCloseableSupplier<Yaml> yaml = snapshotManager.getSnapshotLocalYaml()) {
      dataYaml = yaml.get().load(input);
    } catch (YAMLException ex) {
      // Unchecked exception. Convert to IOException
      throw new IOException(ex);
    }

    if (dataYaml == null) {
      // If Yaml#load returned null, then the file is empty. This is valid yaml
      // but considered an error in this case since we have lost data about
      // the snapshot.
      throw new IOException("Failed to load snapshot file. File is empty.");
    }

    return dataYaml;
  }

  /**
   * Factory class for constructing and pooling instances of the Yaml object.
   * This class extends BasePooledObjectFactory to support object pooling,
   * minimizing the expense of repeatedly creating and destroying Yaml instances.
   *
   * The Yaml instances created by this factory are customized to use a specific
   * set of property and serialization/deserialization configurations.
   * - BeanAccess is configured to access fields directly, allowing manipulation
   *   of private fields in objects.
   * - The PropertyUtils allows read-only properties to be accessed.
   * - Custom Representer and Constructor classes tailored to the OmSnapshotLocalData
   *   data structure are employed to customize how objects are represented in YAML.
   *
   * This class provides thread-safe pooling and management of Yaml instances,
   * ensuring efficient resource usage in high-concurrency environments.
   */
  public static class YamlFactory extends BasePooledObjectFactory<Yaml> {

    @Override
    public Yaml create() {
      PropertyUtils propertyUtils = new PropertyUtils();
      propertyUtils.setBeanAccess(BeanAccess.FIELD);
      propertyUtils.setAllowReadOnlyProperties(true);
      DumperOptions options = new DumperOptions();
      Representer representer = new OmSnapshotLocalDataRepresenter(options);
      representer.setPropertyUtils(propertyUtils);
      SafeConstructor snapshotDataConstructor = new SnapshotLocalDataConstructor();
      return new Yaml(snapshotDataConstructor, representer);
    }

    @Override
    public PooledObject<Yaml> wrap(Yaml yaml) {
      return new DefaultPooledObject<>(yaml);
    }
  }
}
