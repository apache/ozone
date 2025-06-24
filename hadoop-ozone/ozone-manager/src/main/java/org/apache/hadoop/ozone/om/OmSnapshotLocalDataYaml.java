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
import java.util.List;
import java.util.Map;
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
import org.yaml.snakeyaml.introspector.PropertyUtils;
import org.yaml.snakeyaml.nodes.MappingNode;
import org.yaml.snakeyaml.nodes.Node;
import org.yaml.snakeyaml.nodes.Tag;
import org.yaml.snakeyaml.representer.Representer;

/**
 * Class for creating and reading snapshot local properties / data YAML files.
 * Checksum of the YAML fields are computed and stored in the YAML file transparently to callers.
 * Inspired by org.apache.hadoop.ozone.container.common.impl.ContainerDataYaml
 */
public final class OmSnapshotLocalDataYaml extends OmSnapshotLocalData {

  private static final Logger LOG = LoggerFactory.getLogger(OmSnapshotLocalDataYaml.class);

  public static final Tag SNAPSHOT_YAML_TAG = new Tag("OmSnapshotLocalData");

  /**
   * Creates a new OmSnapshotLocalDataYaml with default values.
   */
  public OmSnapshotLocalDataYaml() {
    super();
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
  public static boolean verifyChecksum(OmSnapshotLocalData snapshotData)
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
    final Yaml yaml = getYamlForSnapshotLocalData();

    // Compute new checksum
    snapshotDataCopy.computeAndSetChecksum(yaml);

    // Compare the stored and computed checksums
    String computedChecksum = snapshotDataCopy.getChecksum();
    boolean isValid = storedChecksum.equals(computedChecksum);

    if (!isValid) {
      LOG.warn("Checksum verification failed for snapshot local data. " +
          "Stored: {}, Computed: {}", storedChecksum, computedChecksum);
    }

    return isValid;
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
    }

    private final class ConstructSnapshotLocalData extends AbstractConstruct {
      @SuppressWarnings("unchecked")
      @Override
      public Object construct(Node node) {
        MappingNode mnode = (MappingNode) node;
        Map<Object, Object> nodes = constructMapping(mnode);

        OmSnapshotLocalDataYaml snapshotLocalData = new OmSnapshotLocalDataYaml();

        // Set version from YAML
        Integer version = (Integer) nodes.get(OzoneConsts.OM_SLD_VERSION);
        // Validate version.
        if (version <= 0) {
          // If version is not set or invalid, log a warning, but do not throw.
          LOG.warn("Invalid version ({}) detected in snapshot local data YAML. Proceed with caution.", version);
        }
        snapshotLocalData.setVersion(version);

        // Set other fields from parsed YAML
        snapshotLocalData.setSstFiltered((Boolean) nodes.getOrDefault(OzoneConsts.OM_SLD_IS_SST_FILTERED, false));

        Map<String, List<String>> uncompactedSSTFileList =
            (Map<String, List<String>>) nodes.get(OzoneConsts.OM_SLD_UNCOMPACTED_SST_FILE_LIST);
        if (uncompactedSSTFileList != null) {
          snapshotLocalData.setUncompactedSSTFileList(uncompactedSSTFileList);
        }

        snapshotLocalData.setLastCompactionTime(
            (Long) nodes.getOrDefault(OzoneConsts.OM_SLD_LAST_COMPACTION_TIME, -1L));
        snapshotLocalData.setNeedsCompaction((Boolean) nodes.getOrDefault(OzoneConsts.OM_SLD_NEEDS_COMPACTION, false));

        Map<Integer, Map<String, List<String>>> compactedSSTFileList =
            (Map<Integer, Map<String, List<String>>>) nodes.get(OzoneConsts.OM_SLD_COMPACTED_SST_FILE_LIST);
        if (compactedSSTFileList != null) {
          snapshotLocalData.setCompactedSSTFileList(compactedSSTFileList);
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
  public String getYaml() {
    final Yaml yaml = getYamlForSnapshotLocalData();
    return yaml.dump(this);
  }

  /**
   * Computes checksum (stored in this object), and writes this object to a YAML file.
   * @param yamlFile The file to write to
   * @throws IOException If there's an error writing to the file
   */
  public void writeToYaml(File yamlFile) throws IOException {
    // Create Yaml
    final Yaml yaml = getYamlForSnapshotLocalData();
    // Compute Checksum and update SnapshotData
    computeAndSetChecksum(yaml);
    // Write the SnapshotData with checksum to Yaml file.
    YamlUtils.dump(yaml, this, yamlFile, LOG);
  }

  /**
   * Creates a OmSnapshotLocalDataYaml instance from a YAML file.
   * @param yamlFile The YAML file to read from
   * @return A new OmSnapshotLocalDataYaml instance
   * @throws IOException If there's an error reading the file
   */
  public static OmSnapshotLocalDataYaml getFromYamlFile(File yamlFile) throws IOException {
    Preconditions.checkNotNull(yamlFile, "yamlFile cannot be null");
    try (InputStream inputFileStream = Files.newInputStream(yamlFile.toPath())) {
      return getFromYamlStream(inputFileStream);
    }
  }

  /**
   * Returns a Yaml representation of the snapshot properties.
   * @return Yaml representation of snapshot properties
   */
  public static Yaml getYamlForSnapshotLocalData() {
    PropertyUtils propertyUtils = new PropertyUtils();
    propertyUtils.setBeanAccess(BeanAccess.FIELD);
    propertyUtils.setAllowReadOnlyProperties(true);

    DumperOptions options = new DumperOptions();
    Representer representer = new Representer(options);
    representer.setPropertyUtils(propertyUtils);
    representer.addClassTag(OmSnapshotLocalDataYaml.class, SNAPSHOT_YAML_TAG);

    SafeConstructor snapshotDataConstructor = new SnapshotLocalDataConstructor();
    return new Yaml(snapshotDataConstructor, representer);
  }

  /**
   * Read the YAML content InputStream, and return OmSnapshotLocalDataYaml instance.
   * @throws IOException
   */
  public static OmSnapshotLocalDataYaml getFromYamlStream(InputStream input) throws IOException {
    OmSnapshotLocalDataYaml dataYaml;

    PropertyUtils propertyUtils = new PropertyUtils();
    propertyUtils.setBeanAccess(BeanAccess.FIELD);
    propertyUtils.setAllowReadOnlyProperties(true);

    DumperOptions options = new DumperOptions();
    Representer representer = new Representer(options);
    representer.setPropertyUtils(propertyUtils);

    SafeConstructor snapshotDataConstructor = new SnapshotLocalDataConstructor();

    Yaml yaml = new Yaml(snapshotDataConstructor, representer);

    try {
      dataYaml = yaml.load(input);
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
}
