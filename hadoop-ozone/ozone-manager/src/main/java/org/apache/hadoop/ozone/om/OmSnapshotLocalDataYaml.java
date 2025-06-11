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
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.hadoop.hdds.server.YamlUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.LoaderOptions;
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
import org.yaml.snakeyaml.nodes.Tag;
import org.yaml.snakeyaml.representer.Representer;

/**
 * Class for creating and reading snapshot data YAML files.
 * Checksum of the YAML fields are computed and stored in the YAML file.
 * Inspired by {@link org.apache.hadoop.ozone.container.common.impl.ContainerDataYaml}
 */
public final class OmSnapshotLocalDataYaml {

  private static final Logger LOG =
      LoggerFactory.getLogger(OmSnapshotLocalDataYaml.class);

  public static final Tag SNAPSHOT_YAML_TAG = new Tag("OmSnapshotLocalData");

  private OmSnapshotLocalDataYaml() {

  }

  /**
   * Creates a snapshot data file in YAML format.
   */
  public static void createSnapshotFile(OmSnapshotLocalData snapshotData,
      File snapshotFile) throws IOException {
    // Create Yaml
    final Yaml yaml = getYamlForSnapshotData();
    // Compute Checksum and update SnapshotData
    snapshotData.computeAndSetChecksum(yaml);

    // Write the SnapshotData with checksum to Yaml file.
    YamlUtils.dump(yaml, snapshotData, snapshotFile, LOG);
  }

  /**
   * Read the YAML file, and return snapshotData.
   *
   * @throws IOException
   */
  public static OmSnapshotLocalData readSnapshotFile(File snapshotFile)
      throws IOException {
    Preconditions.checkNotNull(snapshotFile, "snapshotFile cannot be null");
    try (InputStream inputFileStream = Files.newInputStream(
        snapshotFile.toPath())) {
      return readSnapshot(inputFileStream);
    }
  }

  /**
   * Read the YAML file content, and return snapshotData.
   *
   * @throws IOException
   */
  public static OmSnapshotLocalData readSnapshot(byte[] snapshotFileContent)
      throws IOException {
    return readSnapshot(
        new ByteArrayInputStream(snapshotFileContent));
  }

  /**
   * Read the YAML content, and return snapshotData.
   *
   * @throws IOException
   */
  public static OmSnapshotLocalData readSnapshot(InputStream input)
      throws IOException {
    OmSnapshotLocalData snapshotData;
    PropertyUtils propertyUtils = new PropertyUtils();
    propertyUtils.setBeanAccess(BeanAccess.FIELD);
    propertyUtils.setAllowReadOnlyProperties(true);

    Representer representer = new SnapshotDataRepresenter(
        OmSnapshotLocalData.YAML_FIELDS);
    representer.setPropertyUtils(propertyUtils);

    SafeConstructor snapshotDataConstructor = new SnapshotDataConstructor();

    Yaml yaml = new Yaml(snapshotDataConstructor, representer);
    yaml.setBeanAccess(BeanAccess.FIELD);

    try {
      snapshotData = yaml.load(input);
    } catch (YAMLException ex) {
      // Unchecked exception. Convert to IOException
      throw new IOException(ex);
    }

    if (snapshotData == null) {
      // If Yaml#load returned null, then the file is empty. This is valid yaml
      // but considered an error in this case since we have lost data about
      // the snapshot.
      throw new IOException("Failed to load snapshot file. File is empty.");
    }

    return snapshotData;
  }

  /**
   * Returns a Yaml representation of the snapshot properties.
   *
   * @return Yaml representation of snapshot properties
   */
  public static Yaml getYamlForSnapshotData() {
    PropertyUtils propertyUtils = new PropertyUtils();
    propertyUtils.setBeanAccess(BeanAccess.FIELD);
    propertyUtils.setAllowReadOnlyProperties(true);

    Representer representer = new SnapshotDataRepresenter(
        OmSnapshotLocalData.YAML_FIELDS);
    representer.setPropertyUtils(propertyUtils);
    representer.addClassTag(OmSnapshotLocalData.class, SNAPSHOT_YAML_TAG);

    SafeConstructor snapshotDataConstructor = new SnapshotDataConstructor();

    return new Yaml(snapshotDataConstructor, representer);
  }

  /**
   * Representer class to define which fields need to be stored in yaml file.
   */
  private static class SnapshotDataRepresenter extends Representer {

    private List<String> yamlFields;

    SnapshotDataRepresenter(List<String> yamlFields) {
      super(new DumperOptions());
      this.yamlFields = yamlFields;

      // Add custom representer for Timestamp to output epoch milliseconds
      representers.put(Timestamp.class, data -> {
        Timestamp timestamp = (Timestamp) data;
        return represent(timestamp.getTime());
      });
    }

    @Override
    protected Set<Property> getProperties(Class<? extends Object> type) {
      Set<Property> set = super.getProperties(type);
      Set<Property> filtered = new TreeSet<Property>();

      if (type.equals(OmSnapshotLocalData.class)) {
        // filter properties
        for (Property prop : set) {
          String name = prop.getName();
          if (yamlFields.contains(name)) {
            filtered.add(prop);
          }
        }
      }
      return filtered;
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
   * Constructor class for OmSnapshotLocalData, which will be used by Yaml.
   */
  private static class SnapshotDataConstructor extends SafeConstructor {
    SnapshotDataConstructor() {
      super(new LoaderOptions());
      //Adding our own specific constructors for tags.
      this.yamlConstructors.put(
          SNAPSHOT_YAML_TAG, new ConstructSnapshotData());
    }

    private final class ConstructSnapshotData extends AbstractConstruct {
      @SuppressWarnings("unchecked")
      @Override
      public Object construct(Node node) {
        MappingNode mnode = (MappingNode) node;
        Map<Object, Object> nodes = constructMapping(mnode);

        OmSnapshotLocalData snapshotData = new OmSnapshotLocalData();

        // Set fields from parsed YAML
        snapshotData.setSstFiltered((Boolean) nodes.getOrDefault(
            OzoneConsts.IS_SST_FILTERED, false));

        Map<String, List<String>> uncompactedSSTFileList =
            (Map<String, List<String>>) nodes.get(
                OzoneConsts.UNCOMPACTED_SST_FILE_LIST);
        if (uncompactedSSTFileList != null) {
          snapshotData.setUncompactedSSTFileList(uncompactedSSTFileList);
        }

        Object lastCompactionTimeObj = nodes.get(
            OzoneConsts.LAST_COMPACTION_TIME);
        if (lastCompactionTimeObj != null) {
          if (lastCompactionTimeObj instanceof Long) {
            snapshotData.setLastCompactionTime(
                new Timestamp((Long) lastCompactionTimeObj));
          } else if (lastCompactionTimeObj instanceof Timestamp) {
            snapshotData.setLastCompactionTime(
                (Timestamp) lastCompactionTimeObj);
          } else if (lastCompactionTimeObj instanceof String) {
            // Handle timestamp as ISO-8601 string (common in YAML)
            try {
              Instant instant = Instant.parse((String) lastCompactionTimeObj);
              snapshotData.setLastCompactionTime(new Timestamp(instant.toEpochMilli()));
            } catch (Exception e) {
              // If parsing fails, use default timestamp
              snapshotData.setLastCompactionTime(new Timestamp(0));
            }
          }
        }

        snapshotData.setNeedsCompaction((Boolean) nodes.getOrDefault(
            OzoneConsts.NEEDS_COMPACTION, false));

        Map<Integer, Map<String, List<String>>> compactedSSTFileList =
            (Map<Integer, Map<String, List<String>>>) nodes.get(
                OzoneConsts.COMPACTED_SST_FILE_LIST);
        if (compactedSSTFileList != null) {
          snapshotData.setCompactedSSTFileList(compactedSSTFileList);
        }

        String checksum = (String) nodes.get(OzoneConsts.CHECKSUM);
        if (checksum != null) {
          snapshotData.setChecksum(checksum);
        }

        return snapshotData;
      }
    }
  }
}
