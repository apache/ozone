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

package org.apache.hadoop.ozone.container.common.impl;

import static org.apache.hadoop.ozone.OzoneConsts.REPLICA_INDEX;
import static org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData.KEYVALUE_YAML_TAG;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerType;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.server.YamlUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
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
import org.yaml.snakeyaml.nodes.ScalarNode;
import org.yaml.snakeyaml.nodes.Tag;
import org.yaml.snakeyaml.representer.Representer;

/**
 * Class for creating and reading .container files.
 */
public final class ContainerDataYaml {

  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerDataYaml.class);

  private ContainerDataYaml() {

  }

  /**
   * Creates a .container file in yaml format.
   */
  public static void createContainerFile(ContainerData containerData, File containerFile) throws IOException {
    // Create Yaml for given container type
    final Yaml yaml = getYamlForContainerType(containerData.getContainerType(), containerData.getReplicaIndex() > 0);
    // Compute Checksum and update ContainerData
    containerData.computeAndSetContainerFileChecksum(yaml);

    // Write the ContainerData with checksum to Yaml file.
    YamlUtils.dump(yaml, containerData, containerFile, LOG);
  }

  /**
   * Read the yaml file, and return containerData.
   *
   * @throws IOException
   */
  public static ContainerData readContainerFile(File containerFile)
      throws IOException {
    Objects.requireNonNull(containerFile, "containerFile == null");
    try (InputStream inputFileStream = Files.newInputStream(containerFile.toPath())) {
      return readContainer(inputFileStream);
    }

  }

  /**
   * Read the yaml file content, and return containerData.
   *
   * @throws IOException
   */
  public static ContainerData readContainer(byte[] containerFileContent)
      throws IOException {
    return readContainer(
        new ByteArrayInputStream(containerFileContent));
  }

  /**
   * Read the yaml content, and return containerData.
   *
   * @throws IOException
   */
  public static ContainerData readContainer(InputStream input)
      throws IOException {

    ContainerData containerData;
    PropertyUtils propertyUtils = new PropertyUtils();
    propertyUtils.setBeanAccess(BeanAccess.FIELD);
    propertyUtils.setAllowReadOnlyProperties(true);

    Representer representer = new ContainerDataRepresenter(
        KeyValueContainerData.getYamlFields());
    representer.setPropertyUtils(propertyUtils);

    SafeConstructor containerDataConstructor = new ContainerDataConstructor();

    Yaml yaml = new Yaml(containerDataConstructor, representer);
    yaml.setBeanAccess(BeanAccess.FIELD);

    try {
      containerData = yaml.load(input);
    } catch (YAMLException ex) {
      // Unchecked exception. Convert to IOException since an error with one
      // container file is not fatal for the whole thread or datanode.
      throw new IOException(ex);
    }

    if (containerData == null) {
      // If Yaml#load returned null, then the file is empty. This is valid yaml
      // but considered an error in this case since we have lost data about
      // the container.
      throw new IOException("Failed to load container file. File is empty.");
    }

    return containerData;
  }

  /**
   * Given a ContainerType this method returns a Yaml representation of
   * the container properties.
   *
   * @param containerType    type of container
   * @param withReplicaIndex in the container yaml
   * @return Yamal representation of container properties
   * @throws StorageContainerException if the type is unrecognized
   */
  public static Yaml getYamlForContainerType(ContainerType containerType,
      boolean withReplicaIndex)
      throws StorageContainerException {
    PropertyUtils propertyUtils = new PropertyUtils();
    propertyUtils.setBeanAccess(BeanAccess.FIELD);
    propertyUtils.setAllowReadOnlyProperties(true);

    if (containerType == ContainerType.KeyValueContainer) {
      List<String> yamlFields =
          KeyValueContainerData.getYamlFields();
      if (withReplicaIndex) {
        yamlFields = new ArrayList<>(yamlFields);
        yamlFields.add(REPLICA_INDEX);
      }
      Representer representer = new ContainerDataRepresenter(yamlFields);
      representer.setPropertyUtils(propertyUtils);
      representer.addClassTag(
          KeyValueContainerData.class,
          KEYVALUE_YAML_TAG);

      SafeConstructor keyValueDataConstructor = new ContainerDataConstructor();

      return new Yaml(keyValueDataConstructor, representer);
    }
    throw new StorageContainerException("Unrecognized container Type " +
        "format " + containerType, ContainerProtos.Result
        .UNKNOWN_CONTAINER_TYPE);
  }

  /**
   * Representer class to define which fields need to be stored in yaml file.
   */
  private static class ContainerDataRepresenter extends Representer {

    private List<String> yamlFields;

    ContainerDataRepresenter(List<String> yamlFields) {
      super(new DumperOptions());
      this.yamlFields = yamlFields;
    }

    @Override
    protected Set<Property> getProperties(Class<? extends Object> type) {
      Set<Property> set = super.getProperties(type);
      Set<Property> filtered = new TreeSet<Property>();

      // When a new Container type is added, we need to add what fields need
      // to be filtered here
      if (type.equals(KeyValueContainerData.class)) {

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
   * Constructor class for KeyValueData, which will be used by Yaml.
   */
  private static class ContainerDataConstructor extends SafeConstructor {
    ContainerDataConstructor() {
      super(new LoaderOptions());
      //Adding our own specific constructors for tags.
      // When a new Container type is added, we need to add yamlConstructor
      // for that
      this.yamlConstructors.put(
          KEYVALUE_YAML_TAG, new ConstructKeyValueContainerData());
      this.yamlConstructors.put(Tag.INT, new ConstructLong());
    }

    private class ConstructKeyValueContainerData extends AbstractConstruct {
      @Override
      public Object construct(Node node) {
        MappingNode mnode = (MappingNode) node;
        Map<Object, Object> nodes = constructMapping(mnode);

        //Needed this, as TAG.INT type is by default converted to Long.
        long layoutVersion = (long) nodes.get(OzoneConsts.LAYOUTVERSION);
        ContainerLayoutVersion containerLayoutVersion =
            ContainerLayoutVersion.getContainerLayoutVersion(
                (int) layoutVersion);

        long size = (long) nodes.get(OzoneConsts.MAX_SIZE);

        String originPipelineId = (String) nodes.get(
            OzoneConsts.ORIGIN_PIPELINE_ID);
        String originNodeId = (String) nodes.get(OzoneConsts.ORIGIN_NODE_ID);

        //When a new field is added, it needs to be added here.
        KeyValueContainerData kvData = new KeyValueContainerData(
            (long) nodes.get(OzoneConsts.CONTAINER_ID), containerLayoutVersion,
            size, originPipelineId, originNodeId);

        kvData.setContainerDBType((String)nodes.get(
            OzoneConsts.CONTAINER_DB_TYPE));
        kvData.setMetadataPath((String) nodes.get(
            OzoneConsts.METADATA_PATH));
        kvData.setChunksPath((String) nodes.get(OzoneConsts.CHUNKS_PATH));
        Map<String, String> meta = (Map) nodes.get(OzoneConsts.METADATA);
        kvData.setMetadata(meta);
        kvData.setContainerFileChecksum((String) nodes.get(OzoneConsts.CHECKSUM));
        Long timestamp = (Long) nodes.get(OzoneConsts.DATA_SCAN_TIMESTAMP);
        kvData.setDataScanTimestamp(timestamp);
        String state = (String) nodes.get(OzoneConsts.STATE);
        kvData
            .setState(ContainerProtos.ContainerDataProto.State.valueOf(state));
        String schemaVersion = (String) nodes.get(OzoneConsts.SCHEMA_VERSION);
        kvData.setSchemaVersion(schemaVersion);
        final Object replicaIndex = nodes.get(REPLICA_INDEX);
        if (replicaIndex != null) {
          kvData.setReplicaIndex(
              ((Long) replicaIndex).intValue());
        }
        return kvData;
      }
    }

    //Below code is taken from snake yaml, as snakeyaml tries to fit the
    // number if it fits in integer, otherwise returns long. So, slightly
    // modified the code to return long in all cases.
    private class ConstructLong extends AbstractConstruct {
      @Override
      public Object construct(Node node) {
        String value = constructScalar((ScalarNode) node).toString()
            .replaceAll("_", "");
        int sign = +1;
        char first = value.charAt(0);
        if (first == '-') {
          sign = -1;
          value = value.substring(1);
        } else if (first == '+') {
          value = value.substring(1);
        }
        int base = 10;
        if ("0".equals(value)) {
          return Long.valueOf(0);
        } else if (value.startsWith("0b")) {
          value = value.substring(2);
          base = 2;
        } else if (value.startsWith("0x")) {
          value = value.substring(2);
          base = 16;
        } else if (value.startsWith("0")) {
          value = value.substring(1);
          base = 8;
        } else if (value.indexOf(':') != -1) {
          String[] digits = value.split(":");
          int bes = 1;
          int val = 0;
          for (int i = 0, j = digits.length; i < j; i++) {
            val += (Long.parseLong(digits[(j - i) - 1]) * bes);
            bes *= 60;
          }
          return createNumber(sign, String.valueOf(val), 10);
        } else {
          return createNumber(sign, value, 10);
        }
        return createNumber(sign, value, base);
      }
    }

    private Number createNumber(int sign, String number, int radix) {
      Number result;
      if (sign < 0) {
        number = "-" + number;
      }
      result = Long.valueOf(number, radix);
      return result;
    }
  }

}
